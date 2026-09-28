# Uber Go Style Guide: Top 10

The ten rules from the [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) most likely to prevent real bugs, in order of importance.

## 1. Handle errors once
[Handle Errors Once](https://github.com/uber-go/guide/blob/master/style.md#handle-errors-once) · [Error Wrapping](https://github.com/uber-go/guide/blob/master/style.md#error-wrapping)

For each error, do exactly one thing: return it (wrapped or as-is), or log it and degrade gracefully. Never both.

**Why:** callers higher up the stack will usually log the error too, so logging and returning fills the logs with the same failure several times over. When you wrap, keep the context short (`"new store: %w"`, not `"failed to create new store: %w"`). Otherwise every layer adds another "failed to", and the message grows into a pile of them.

```go
// Bad
if err != nil {
    log.Printf("could not get user %q: %v", id, err)
    return err
}

// Good
if err != nil {
    return fmt.Errorf("get user %q: %w", id, err)
}
```

## 2. Choose error types deliberately
[Error Types](https://github.com/uber-go/guide/blob/master/style.md#error-types) · [Error Naming](https://github.com/uber-go/guide/blob/master/style.md#error-naming)

| Caller needs to match? | Message | Use |
|---|---|---|
| No | static | `errors.New` |
| No | dynamic | `fmt.Errorf` |
| Yes | static | top-level `var ErrX = errors.New(...)` |
| Yes | dynamic | custom `error` type (`XError`) |

**Why:** an exported error variable or type becomes part of your package's public API, so only create one when callers actually need to branch on it. Wrap with `%w` when callers should be able to see the underlying error, and with `%v` when that error is an implementation detail you want to hide.

## 3. Don't panic, and only exit in `main()`
[Don't Panic](https://github.com/uber-go/guide/blob/master/style.md#dont-panic) · [Exit in Main](https://github.com/uber-go/guide/blob/master/style.md#exit-in-main) · [Exit Once](https://github.com/uber-go/guide/blob/master/style.md#exit-once)

Return errors instead of panicking. Call `os.Exit` or `log.Fatal` only from `main()`, ideally just once, by moving the logic into a `run() error` function.

**Why:** panics are a major cause of cascading failures, and panic/recover is not an error-handling strategy. Exiting from deep inside the code has three problems:
- Any function can end the program, so control flow is hard to follow.
- A test that reaches that code exits along with it.
- `defer`red cleanup never runs.

```go
func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

## 4. Use the comma-ok form for type assertions
[Handle Type Assertion Failures](https://github.com/uber-go/guide/blob/master/style.md#handle-type-assertion-failures)

**Why:** the single-value form panics when the type is wrong, which brings back all the problems in #3.

```go
// Bad
t := i.(string)

// Good
t, ok := i.(string)
if !ok {
    // handle gracefully
}
```

## 5. Copy slices and maps at boundaries
[Copy Slices and Maps at Boundaries](https://github.com/uber-go/guide/blob/master/style.md#copy-slices-and-maps-at-boundaries)

Copy a slice or map you receive before storing it, and return a copy of internal state rather than the original.

**Why:** slices and maps hold pointers to their underlying data. If you store a caller's slice, the caller can still change it after the fact. If you return your internal map, callers can change it without going through your methods, and without taking your mutex.

```go
func (d *Driver) SetTrips(trips []Trip) {
    d.trips = make([]Trip, len(trips))
    copy(d.trips, trips)
}
```

## 6. Don't fire-and-forget goroutines
[Don't fire-and-forget goroutines](https://github.com/uber-go/guide/blob/master/style.md#dont-fire-and-forget-goroutines) · [Wait for goroutines to exit](https://github.com/uber-go/guide/blob/master/style.md#wait-for-goroutines-to-exit) · [No goroutines in `init()`](https://github.com/uber-go/guide/blob/master/style.md#no-goroutines-in-init)

Every goroutine needs a predictable stop time or a way to be told to stop, *and* a way for the caller to wait until it has finished (for example a `sync.WaitGroup` or a `done` channel).

**Why:** goroutines are cheap but not free. A leaked goroutine keeps its stack, can prevent other objects from being garbage collected, and holds on to resources indefinitely. Starting goroutines in `init()` gives users no control over their lifetime. Use [goleak](https://pkg.go.dev/go.uber.org/goleak) in tests to catch leaks.

```go
stop := make(chan struct{})
done := make(chan struct{})
go func() {
    defer close(done)
    for {
        select {
        case <-ticker.C:
            flush()
        case <-stop:
            return
        }
    }
}()

// Elsewhere...
close(stop) // signal the goroutine to stop
<-done      // and wait for it to exit
```

## 7. Avoid mutable globals and `init()`
[Avoid Mutable Globals](https://github.com/uber-go/guide/blob/master/style.md#avoid-mutable-globals) · [Avoid `init()`](https://github.com/uber-go/guide/blob/master/style.md#avoid-init)

Use dependency injection instead of mutating package-level state, and that includes function variables like `_timeNow`. If you really need `init()`, it must be deterministic, must not depend on other `init()`s, and must do no I/O and no reading of environment or global state.

**Why:** tests that swap out globals can't run in parallel and have to remember to restore them. `init()` runs implicitly on import, so any side effects happen in every program and every test that imports the package, whether or not they want them.

## 8. Verify interface compliance at compile time
[Verify Interface Compliance](https://github.com/uber-go/guide/blob/master/style.md#verify-interface-compliance)

```go
var _ http.Handler = (*Handler)(nil)
```

**Why:** without this line, a method signature that drifts only shows up at the call site that needs the interface, which may be in another package or only happen at runtime. With it, the build fails right next to the type. The line costs nothing at runtime.

## 9. Use zero-value mutexes, don't embed them, and release with `defer`
[Zero-value Mutexes are Valid](https://github.com/uber-go/guide/blob/master/style.md#zero-value-mutexes-are-valid) · [Defer to Clean Up](https://github.com/uber-go/guide/blob/master/style.md#defer-to-clean-up)

```go
type SMap struct {
    mu   sync.Mutex // not embedded, not a pointer
    data map[string]string
}

func (m *SMap) Get(k string) string {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.data[k]
}
```

**Why:** a zero-value `sync.Mutex` is ready to use, so `new(sync.Mutex)` just adds a pointer. Embedding the mutex promotes `Lock` and `Unlock` into your type's public API, which lets callers break your locking. `defer` has very little overhead and ensures the unlock or close happens on every return path, including ones added later.

## 10. Reduce nesting
[Reduce Nesting](https://github.com/uber-go/guide/blob/master/style.md#reduce-nesting) · [Unnecessary Else](https://github.com/uber-go/guide/blob/master/style.md#unnecessary-else)

Handle errors and special cases first, then `return` or `continue`. Drop `else` when both branches only set the same variable.

**Why:** code with less indentation is easier to follow. Keeping the main path at the left margin makes each exit condition obvious, and makes it harder to miss a case.

```go
// Bad
a := 10
if b {
    a = 100
} else {
    a = 10
}

// Good
a := 10
if b {
    a = 100
}
```

---

## Runners-up

- [Channel Size is One or None](https://github.com/uber-go/guide/blob/master/style.md#channel-size-is-one-or-none): larger buffers hide backpressure problems, so any other size needs a clear justification.
- [Start Enums at One](https://github.com/uber-go/guide/blob/master/style.md#start-enums-at-one): `iota + 1` makes the zero value mean "unset" instead of a real option.
- [Use `"time"` to handle time](https://github.com/uber-go/guide/blob/master/style.md#use-time-to-handle-time): use `time.Time` and `time.Duration`, never bare ints. If an int is unavoidable, put the unit in the name (`IntervalMillis`).
- [Avoid Embedding Types in Public Structs](https://github.com/uber-go/guide/blob/master/style.md#avoid-embedding-types-in-public-structs): embedding leaks implementation details, and any later change to the embedded type breaks callers.
- [Use field tags in marshaled structs](https://github.com/uber-go/guide/blob/master/style.md#use-field-tags-in-marshaled-structs): with explicit tags, renaming a field can't silently change your wire format.
- [Test Tables](https://github.com/uber-go/guide/blob/master/style.md#test-tables): keep the test logic in one place and give each case its own row.
- [Functional Options](https://github.com/uber-go/guide/blob/master/style.md#functional-options): add optional constructor settings without breaking the API.
- [Prefer strconv over fmt](https://github.com/uber-go/guide/blob/master/style.md#prefer-strconv-over-fmt) · [Prefer Specifying Container Capacity](https://github.com/uber-go/guide/blob/master/style.md#prefer-specifying-container-capacity): cheap performance wins on hot paths.
