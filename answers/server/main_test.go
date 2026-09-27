// Tests for the HTTP server.
//
// Run them with:  go test -race ./answers/server
//
// # Two ways to test HTTP code in Go (ELI5)
//
//  1. The pretend way (most of these tests): httptest.NewRequest builds a
//     fake request, httptest.NewRecorder is a fake response that just
//     remembers whatever gets written to it, and you call
//     handler.ServeHTTP(recorder, request) directly. No network and no
//     ports. It's fast, and you can inspect everything afterward.
//  2. The real way (TestGracefulShutdown): start the actual server on a real
//     port and send it real HTTP requests. Slower, but it's the only way to
//     test things like "does shutdown really let in-flight requests finish?"
//
// Because the pretend way runs the handler on the test's own goroutine, it
// also works inside synctest's fake clock, so a request that "takes 30
// seconds" finishes instantly.
package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// syncBuffer is a bytes.Buffer that's safe for the concurrent writes a
// real server makes to its log.
//
// A plain bytes.Buffer isn't safe to use from several goroutines at once,
// and a real server handles each request on its own goroutine. Wrapping
// every method in a mutex fixes that. The race detector would complain
// without it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testLogger returns a logger that writes into a buffer instead of the
// terminal, so tests can read back what was logged and check it.
//
// syncBuffer has a Write method, so it satisfies io.Writer, which is all
// slog needs. In Go you never write "implements io.Writer." Having the right
// methods is enough.
func testLogger() (*slog.Logger, *syncBuffer) {
	var buf syncBuffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// Order every dish on the menu (and a few that aren't), and check what
// comes back and how long it took.
func TestRoutes(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   string // substring
		wantTime   time.Duration
	}{
		{"healthz", "GET", "/healthz", 200, "ok", 0},
		{"wrong method", "POST", "/healthz", 405, "", 0}, // 405 = Method Not Allowed
		{"unknown path", "GET", "/nope", 404, "", 0},     // 404 = Not Found
		{"slow completes", "GET", "/slow?d=5s", 200, "slept 5s", 5 * time.Second},
		{"slow zero", "GET", "/slow?d=0s", 200, "slept 0s", 0},
		// Asks for 45s but the timeout is 30s: we get 503 Service
		// Unavailable after exactly 30s.
		{"slow hits timeout", "GET", "/slow?d=45s", 503, "timed out", requestTimeout},
		{"slow missing d", "GET", "/slow", 400, "duration", 0}, // 400 = Bad Request
		{"slow bad d", "GET", "/slow?d=soon", 400, "duration", 0},
		{"slow negative d", "GET", "/slow?d=-1s", 400, "duration", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// httptest.NewRecorder calls the handler on this goroutine, so
			// inside a synctest bubble the handler's timers use fake time:
			// the 30s timeout case finishes instantly.
			synctest.Test(t, func(t *testing.T) {
				logger, _ := testLogger() // _ = "I don't need the log buffer here"
				h := newHandler(logger, requestTimeout)

				req := httptest.NewRequest(tt.method, tt.target, nil) // nil = no request body
				rec := httptest.NewRecorder()
				start := time.Now()
				h.ServeHTTP(rec, req) // "serve" the fake request

				// The recorder remembers everything: rec.Code is the status,
				// and rec.Body is what was written.
				if rec.Code != tt.wantStatus {
					t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
				}
				if !strings.Contains(rec.Body.String(), tt.wantBody) {
					t.Errorf("body = %q, want it to contain %q", rec.Body.String(), tt.wantBody)
				}
				if got := time.Since(start); got != tt.wantTime {
					t.Errorf("took %v, want %v", got, tt.wantTime)
				}
			})
		})
	}
}

// If the customer walks out, the cook should stop cooking, and not bother
// plating the food for nobody.
func TestSlowStopsWhenClientGoesAway(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Pretend the client hangs up 2 seconds in. In a real server,
		// net/http cancels r.Context() when the connection drops. Here we
		// cancel it ourselves.
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(2*time.Second, cancel)

		req := httptest.NewRequestWithContext(ctx, "GET", "/slow?d=1m", nil)
		rec := httptest.NewRecorder()
		start := time.Now()
		handleSlow(rec, req) // call the handler directly, with no middleware

		if got := time.Since(start); got != 2*time.Second {
			t.Errorf("handler returned after %v, want 2s", got)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("wrote %q to a client that had gone away", rec.Body.String())
		}
	})
}

func TestRequestID(t *testing.T) {
	// A fake "next" handler that just records what request ID it saw. This
	// lets us test withRequestID on its own, separate from everything else.
	var seen string
	h := withRequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))

	t.Run("reuses incoming header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Request-ID", "abc123")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if seen != "abc123" {
			t.Errorf("handler saw ID %q, want abc123", seen)
		}
		if got := rec.Header().Get("X-Request-ID"); got != "abc123" {
			t.Errorf("response header = %q, want abc123", got)
		}
	})

	t.Run("generates unique IDs", func(t *testing.T) {
		// A map used as a set: put each ID in as a key, then count the keys.
		// If any ID repeated, there'd be fewer than 100.
		ids := map[string]bool{}
		for range 100 {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
			id := rec.Header().Get("X-Request-ID")
			if id == "" || id != seen {
				t.Fatalf("header %q and context %q should match and be non-empty", id, seen)
			}
			ids[id] = true
		}
		if len(ids) != 100 {
			t.Errorf("got %d unique IDs out of 100", len(ids))
		}
	})
}

// Every request should produce a log line with the right status, however
// the handler went about setting it.
func TestLogRequests(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		// 418 is "I'm a teapot," a real (joke) status code, handy in tests
		// because nothing produces it by accident.
		{"explicit status", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }, "status=418"},
		// Writing a body without calling WriteHeader first means 200.
		{"implicit 200 from Write", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "hi") }, "status=200"},
		// A handler that writes nothing at all also means 200.
		{"nothing written", func(w http.ResponseWriter, r *http.Request) {}, "status=200"},
		{
			// Once the body has started, the 200 is already on the wire and
			// net/http ignores the late WriteHeader. The log must agree.
			"late WriteHeader is ignored",
			func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "partial")
				w.WriteHeader(http.StatusInternalServerError)
			},
			"status=200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, buf := testLogger()
			h := withRequestID(logRequests(logger)(tt.handler))

			req := httptest.NewRequest("GET", "/some/path", nil)
			req.Header.Set("X-Request-ID", "req-1")
			h.ServeHTTP(httptest.NewRecorder(), req)

			// Check the log line has every piece it should.
			for _, want := range []string{tt.want, "method=GET", "path=/some/path", "request_id=req-1", "duration="} {
				if !strings.Contains(buf.String(), want) {
					t.Errorf("log %q is missing %q", buf.String(), want)
				}
			}
		})
	}
}

// A handler that panics should produce a 500 and a log entry, not a crash.
func TestRecoverPanics(t *testing.T) {
	logger, buf := testLogger()
	// A deliberately broken "cook," wrapped in the fire marshal
	// (recoverPanics), wrapped in the logbook (logRequests).
	h := logRequests(logger)(recoverPanics(logger)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { panic("kaboom") },
	)))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	logs := buf.String()
	for _, want := range []string{"panic", "kaboom", "status=500"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log is missing %q:\n%s", want, logs)
		}
	}
}

// The one panic recoverPanics must NOT swallow: http.ErrAbortHandler is a
// deliberate signal to net/http, so it has to be passed along.
func TestRecoverPanicsLetsAbortHandlerThrough(t *testing.T) {
	logger, _ := testLogger()
	h := recoverPanics(logger)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) },
	))

	// We expect the panic to come back out, so the test sets up its own
	// safety net and checks that it caught the right thing.
	defer func() {
		if v := recover(); v != http.ErrAbortHandler {
			t.Errorf("recovered %v, want http.ErrAbortHandler to be re-panicked", v)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
}

// TestGracefulShutdown runs the real server on a real socket, so it uses
// real time rather than synctest.
//
// ELI5 of the plan: open the restaurant, seat a customer who's ordered a
// half-second meal, announce closing time while they're mid-meal, and check
// that (1) they got to finish, (2) the restaurant then closed cleanly, and
// (3) the front door really is locked afterward.
func TestGracefulShutdown(t *testing.T) {
	// Port 0 means "any free port, you pick." That avoids clashing with
	// anything else on the machine, or with other tests running at the
	// same time.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "http://" + ln.Addr().String() // e.g. http://127.0.0.1:54321
	logger, logs := testLogger()

	// Start the server in the background. Cancelling ctx will play the role
	// of pressing Ctrl-C.
	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() { runErr <- run(ctx, logger, ln) }()

	// Start a request that will still be in flight when shutdown begins.
	// A small struct type declared inside the function, just for carrying
	// the request's outcome back over a channel.
	type result struct {
		status int
		body   string
		err    error
	}
	inFlight := make(chan result, 1)
	go func() {
		resp, err := http.Get(addr + "/slow?d=500ms")
		if err != nil {
			inFlight <- result{err: err}
			return
		}
		defer resp.Body.Close() // always close response bodies, or connections leak
		body, err := io.ReadAll(resp.Body)
		inFlight <- result{resp.StatusCode, string(body), err}
	}()

	// Give the request time to reach the handler, then "press Ctrl-C".
	time.Sleep(200 * time.Millisecond)
	cancel()

	// (1) The customer mid-meal got to finish.
	res := <-inFlight
	if res.err != nil {
		t.Fatalf("in-flight request failed during shutdown: %v", res.err)
	}
	if res.status != http.StatusOK || !strings.Contains(res.body, "slept") {
		t.Errorf("in-flight request got %d %q, want 200 slept", res.status, res.body)
	}

	// (2) run returned nil, meaning a clean shutdown. The time.After case
	// is a safety net, so a broken shutdown fails after 5s instead of
	// hanging the test forever.
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after shutdown")
	}

	// (3) The listener is closed, so new connections must fail.
	client := &http.Client{Timeout: time.Second}
	if _, err := client.Get(addr + "/healthz"); err == nil {
		t.Error("server still accepted connections after shutdown")
	}
	if !strings.Contains(logs.String(), "stopped cleanly") {
		t.Errorf("logs are missing the clean-stop message:\n%s", logs.String())
	}
}

// If the server can't even start serving, run should report a real error,
// not pretend everything is fine.
func TestRunReportsServeFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // Serve will fail immediately on a closed listener
	// (Like handing the restaurant a front door that's already bricked up.)

	logger, _ := testLogger()
	err = run(t.Context(), logger, ln)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("run returned %v, want a real serve error", err)
	}
}
