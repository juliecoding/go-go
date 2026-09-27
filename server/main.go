// Command server is exercise 4: an HTTP server with middleware, request
// timeouts, and graceful shutdown.
//
// Run it with `go run ./server`.
//
// Routes (http.ServeMux with Go 1.22+ method patterns)
//
//	GET /healthz    -> 200 "ok"
//	GET /slow?d=3s  -> sleeps for d, returning early if r.Context() is cancelled
//
// Middleware (plain func(http.Handler) http.Handler, composed)
//   - Request ID: reuse the X-Request-ID header or generate one, put it in
//     the request context, and echo it in the response.
//   - Logging with log/slog: method, path, status, duration, request ID.
//     Capturing the status means wrapping http.ResponseWriter.
//   - Panic recovery that logs the panic and returns a 500.
//
// Requirements
//   - A per-request timeout. Know the difference between http.TimeoutHandler
//     and wrapping r.Context() with context.WithTimeout.
//   - Set ReadHeaderTimeout, ReadTimeout, WriteTimeout, and IdleTimeout on
//     http.Server, and be able to say what each one protects against.
//   - Graceful shutdown: signal.NotifyContext for SIGINT and SIGTERM, then
//     srv.Shutdown with a 10s deadline. Exit non-zero if shutdown fails.
//
// Done when
//   - `curl localhost:8080/slow?d=5s`, then Ctrl-C the server mid-request:
//     the request still completes, and the server then exits cleanly.
//
// Stretch
//   - Add the exercise 3 rate limiter as middleware (429 + Retry-After).
//   - GET /readyz that flips to 503 as soon as shutdown begins.
//   - Tests with net/http/httptest for each middleware.
//
// Talking points
//   - Why srv.ListenAndServe returning http.ErrServerClosed isn't a failure.
//   - What Shutdown does and doesn't wait for (hijacked and WebSocket
//     connections, and RegisterOnShutdown).
//   - Why the zero-value http.Server with no timeouts is risky in production
//     (Slowloris).
package main

func main() {
	// TODO
}
