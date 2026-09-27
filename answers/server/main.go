// Command server is the answer to exercise 4: an HTTP server with
// middleware, request timeouts, and graceful shutdown.
//
// Run it with `go run ./answers/server`, then try:
//
//	curl -i localhost:8080/healthz
//	curl -i 'localhost:8080/slow?d=5s'   # then Ctrl-C the server mid-request
//	curl -i 'localhost:8080/slow?d=45s'  # hits the 30s request timeout
//
// # The big picture (ELI5)
//
// Think of the server as a restaurant.
//
//   - Routes are the menu. "GET /healthz" is one dish, and "GET /slow" is
//     another. Order something that isn't on the menu and you get a 404.
//   - Handlers are the cooks. Each one makes one dish: it reads the request
//     (the order) and writes the response (the plate).
//   - Middleware is the staff standing between the front door and the
//     kitchen. Every order passes through each of them in turn:
//     a host who tags every order with a ticket number (the request ID), a
//     manager who writes every order and its outcome in a logbook (logging),
//     a fire marshal who keeps one kitchen accident from burning down the
//     whole restaurant (panic recovery), and a timer that tells a cook
//     "you have 30 seconds for this dish" (the request timeout).
//   - Graceful shutdown is closing time. You lock the front door so no new
//     customers come in, let the people already eating finish their meals,
//     and then turn out the lights. The alternative is flipping off the
//     lights while people are mid-bite.
//
// # Design notes (the interview version)
//
//   - main is tiny; the real work is in run, which takes a context and a
//     listener. Tests pass a listener on a random port and cancel the context
//     instead of sending a signal.
//   - Middleware is plain func(http.Handler) http.Handler. The order matters:
//     request ID is outermost so every log line has an ID, and logging sits
//     outside panic recovery so a recovered panic is logged as a 500.
//   - The request timeout uses context.WithTimeout on r.Context(). That is
//     cooperative: handlers have to watch ctx.Done(). http.TimeoutHandler is
//     the other option. It enforces the timeout even on handlers that ignore
//     ctx, by buffering the response and replying 503 itself, but it doesn't
//     support streaming or http.Flusher.
//   - Shutdown stops accepting new connections, closes idle ones, and waits
//     for in-flight requests. It does not wait for hijacked connections such
//     as WebSockets; use srv.RegisterOnShutdown to tell those to close.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"
)

const (
	requestTimeout  = 30 * time.Second // the longest any one request may take
	shutdownTimeout = 10 * time.Second // how long closing time may take
)

// main is where the program starts. It's kept as small as possible,
// because main is hard to test: it can't take arguments or return an error,
// and os.Exit kills the test run along with everything else. So main just
// wires things together and hands off to run, which is easy to test.
func main() {
	// slog (Go 1.21+) is the standard structured logger. It writes lines
	// like: level=INFO msg=request method=GET path=/healthz status=200
	// key=value pairs instead of free-form text, so logs are searchable.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Open the front door: start listening for connections on port 8080.
	// Doing this here, instead of inside run, is what lets tests hand run a
	// different door (a random free port).
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1) // a non-zero exit code tells the shell "this failed"
	}
	if err := run(context.Background(), logger, ln); err != nil {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}

// run serves on ln until ctx is cancelled or the process receives SIGINT or
// SIGTERM, then shuts down gracefully.
//
// SIGINT is what Ctrl-C sends. SIGTERM is what Docker, Kubernetes, and
// systemd send when they want a program to stop politely. Both mean "please
// wrap up and exit," so we treat them the same way.
func run(ctx context.Context, logger *slog.Logger, ln net.Listener) error {
	// NotifyContext returns a context that gets cancelled when one of those
	// signals arrives. It turns "Ctrl-C was pressed" into the same
	// ctx.Done() signal everything else in Go already understands.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := newServer(logger, requestTimeout)

	// srv.Serve blocks: it runs until the server is shut down. So we run it
	// in its own goroutine, which leaves this goroutine free to watch for
	// the stop signal. The channel carries Serve's final error back to us.
	serveErr := make(chan error, 1) // buffered so the goroutine can always exit
	// (With a buffer of 1, the goroutine can drop its error off and leave
	// even if nobody is reading at that moment. Without the buffer, it could
	// be stuck forever holding its error: a leaked goroutine.)
	go func() { serveErr <- srv.Serve(ln) }()
	logger.Info("listening", "addr", ln.Addr().String())

	// Now wait for whichever comes first:
	//   - Serve fails on its own (something went wrong), or
	//   - we're told to stop (Ctrl-C, SIGTERM, or a test cancelling ctx).
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err) // failed without being asked to stop
	case <-ctx.Done():
	}

	// Restore default signal handling, so a second Ctrl-C kills the process
	// immediately instead of waiting out the shutdown.
	// (If shutdown is taking too long, an impatient human can press Ctrl-C
	// again and the program dies right away.)
	stop()
	logger.Info("shutting down", "timeout", shutdownTimeout)

	// Use a fresh context: ctx is already cancelled, and Shutdown would give
	// up at once if handed a cancelled context.
	// (Passing ctx here is a common bug. Shutdown would see an already-
	// cancelled context and quit immediately, cutting off the diners.)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Closing time: stop accepting new connections, then wait for in-flight
	// requests to finish. If they're not done after 10s, give up and return
	// an error.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	// After Shutdown, Serve returns http.ErrServerClosed. That's the normal,
	// successful ending, not a failure.
	// (It's a little odd that the success case arrives as an "error," but
	// that's how net/http reports it. Treating it as a failure is a classic
	// mistake.)
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	logger.Info("stopped cleanly")
	return nil // nil error = success
}

// newServer builds the http.Server with its timeouts set.
func newServer(logger *slog.Logger, timeout time.Duration) *http.Server {
	return &http.Server{
		Handler: newHandler(logger, timeout),

		// Without these, the zero-value server waits forever on slow clients,
		// and a Slowloris attack can hold every connection open.
		//
		// ELI5 of Slowloris: a prankster sits at every table and orders one
		// word at a time... very... slowly. The restaurant fills up with
		// people who never finish ordering, and real customers can't get a
		// seat. These timeouts are the rule "you have 5 seconds to order."
		ReadHeaderTimeout: 5 * time.Second,  // time to send the request headers
		ReadTimeout:       10 * time.Second, // time to send the whole request, body included
		// WriteTimeout is how long we have to send the response back. It
		// must be longer than the request timeout, or a slow-but-legal
		// request would get cut off before it could reply.
		WriteTimeout: timeout + 5*time.Second,
		IdleTimeout:  2 * time.Minute, // how long a keep-alive connection may sit idle

		// The server's own internal complaints go to our structured logger
		// instead of the old log package.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

// newHandler builds the routes and wraps them in middleware.
func newHandler(logger *slog.Logger, timeout time.Duration) http.Handler {
	// The menu. Since Go 1.22, patterns can include the HTTP method
	// ("GET /healthz"). A POST to /healthz automatically gets
	// 405 Method Not Allowed. Before 1.22 you had to check r.Method yourself.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /slow", handleSlow)

	// Applied inside out: the last wrapper runs first on each request.
	//
	// ELI5: it's like wrapping a present. mux is the gift. Each line wraps
	// another layer of paper around everything so far. Whoever opens it
	// (an incoming request) tears through the outermost layer first:
	//
	//   request → withRequestID → logRequests → recoverPanics → withTimeout → mux
	//
	// and the response travels back out in the reverse order.
	var h http.Handler = mux
	h = withTimeout(timeout)(h)
	h = recoverPanics(logger)(h)
	h = logRequests(logger)(h)
	h = withRequestID(h)
	return h
}

// handleHealthz is the simplest possible handler. Load balancers and
// Kubernetes call a URL like this to ask "are you alive?"
//
// Every handler has this shape: w is where you write the response, and r is
// the request that came in.
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok") // writing to w is writing the response body
}

// handleSlow sleeps for however long ?d= asks for. It exists to have
// something slow to cancel, time out, and shut down in the middle of.
func handleSlow(w http.ResponseWriter, r *http.Request) {
	// r.URL.Query().Get("d") reads ?d=... from the URL, and ParseDuration
	// turns "3s" into 3 seconds. Never trust input: anything we can't parse
	// gets a 400 Bad Request.
	d, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil || d < 0 {
		http.Error(w, `query parameter "d" must be a duration such as 3s`, http.StatusBadRequest)
		return
	}

	timer := time.NewTimer(d)
	defer timer.Stop() // if we leave early, don't leave the timer ticking

	// Wait for whichever comes first: the nap finishing, or the request's
	// context ending. Every request carries a context (r.Context()), and
	// net/http cancels it automatically if the client hangs up.
	select {
	case <-timer.C:
		fmt.Fprintf(w, "slept %v\n", d)
	case <-r.Context().Done():
		// Two ways to get here: the timeout middleware's deadline passed, or
		// the client went away. Only the first one has anyone to reply to.
		// (Nobody is listening for a reply to a client that hung up.)
		if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
			http.Error(w, "request timed out", http.StatusServiceUnavailable)
		}
	}
}

// --- middleware ---
//
// Every middleware below has the same shape:
//
//	func(next http.Handler) http.Handler
//
// "Give me the next thing in line, and I'll give you back a new handler that
// does my extra job and then calls next." It's one link in a chain.

// ctxKey is a private type for context keys.
//
// Why not just use the string "requestID" as the key? Because any other
// package could also use the string "requestID", and the two would clobber
// each other. A key of an unexported type can't collide with anyone else's,
// since nobody outside this package can even create one.
type ctxKey int

const requestIDKey ctxKey = iota // iota just means 0 here; it counts up in const blocks

// RequestIDFrom returns the request ID stored by withRequestID, if any.
func RequestIDFrom(ctx context.Context) string {
	// ctx.Value returns an `any`, so we assert it back to a string. The
	// ", _" form returns "" instead of panicking if there's no ID.
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// withRequestID reuses an incoming X-Request-ID header or generates one,
// puts it in the request context, and echoes it in the response.
//
// ELI5: the host staples a ticket number to every order. When something goes
// wrong, you can find every log line for that one order by searching for its
// number, even across several services, if they all pass the header along.
func withRequestID(next http.Handler) http.Handler {
	// http.HandlerFunc turns an ordinary function into an http.Handler.
	// It's a type conversion, not a function call.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = rand.Text() // Go 1.24+: 26 random base32 characters
		}
		w.Header().Set("X-Request-ID", id)
		// Contexts can't be changed, only extended. WithValue makes a new
		// context that has everything the old one had plus our ID, and
		// r.WithContext makes a copy of the request that carries it.
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// statusRecorder wraps a ResponseWriter to remember the status code.
//
// Why this is needed: http.ResponseWriter lets you write a status code, but
// it has no way to read it back afterward. So the logging middleware slips
// this wrapper in place of the real writer. It passes everything through
// untouched, but makes a note of the status on the way.
//
// Embedding http.ResponseWriter (a field with a type but no name) means
// statusRecorder automatically gets all of the real writer's methods. We
// only override the two we care about. That's Go's version of "inherit
// everything, override a couple," done by composition rather than
// inheritance.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	// Only the first status counts. Once headers are sent they can't be
	// taken back, and net/http ignores (and logs a warning about) any later
	// WriteHeader call. We copy that behavior so our log matches what the
	// client actually got.
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK // an implicit 200, same as net/http does
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.NewResponseController reach the underlying writer's
// Flush, Hijack, and deadline methods through this wrapper.
// (Without it, wrapping the writer would quietly hide features that
// streaming responses and WebSockets depend on.)
func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// logRequests writes one log line per request: what was asked for, what
// came back, how long it took, and the ticket number.
//
// logRequests(logger) returns a middleware. That's one extra layer of
// function compared to withRequestID, because this one needs a logger
// handed to it first. A function that returns a function that returns a
// handler is a mouthful, but it's a standard Go pattern.
func logRequests(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r) // run everything further down the chain
			if rec.status == 0 {
				rec.status = http.StatusOK // handler wrote nothing at all
			}
			logger.InfoContext(r.Context(), "request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration", time.Since(start),
				"request_id", RequestIDFrom(r.Context()),
			)
		})
	}
}

// recoverPanics catches a panic in any handler below it and turns it into a
// 500 response, instead of crashing or dropping the connection.
//
// ELI5: a panic is Go's "something went horribly, unexpectedly wrong" (like
// reading past the end of a slice, or using a nil pointer). Normally a panic
// crashes the whole program. recover() is the safety net: called inside a
// deferred function, it catches the panic and lets you carry on.
func recoverPanics(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// This deferred function runs when ServeHTTP returns, whether it
			// returned normally or because something panicked.
			defer func() {
				v := recover()
				if v == nil {
					return // no panic, nothing to do
				}
				// http.ErrAbortHandler is net/http's deliberate "abort this
				// response" panic. Let the server handle it.
				if v == http.ErrAbortHandler {
					panic(v)
				}
				logger.ErrorContext(r.Context(), "panic",
					"value", v,
					"request_id", RequestIDFrom(r.Context()),
					"stack", string(debug.Stack()), // where the panic happened
				)
				// If the handler already sent headers this can't change the
				// status, but the client still gets a truncated response.
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// withTimeout gives every request a deadline: "this dish must be ready in
// 30 seconds." Handlers see it through r.Context(), and polite handlers
// (like handleSlow) stop work when it expires.
func withTimeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel() // always release the timer once the request is done
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
