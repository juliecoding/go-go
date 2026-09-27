# Drills

CoderPad-style practice: every file is a standalone `package main`. No fixes or answers are included.

Each file has `//go:build ignore` so it stays out of `go build ./...` / `go vet ./...`. Run it by path:

```bash
go run drills/debug/d01_counter.go
go run -race drills/debug/d01_counter.go
```

For CoderPad realism: plain editor, no autocomplete, imports typed by hand.

## Debug drills (`debug/`), ~15 min each

Each program has a bug. Talk out loud as you go:

1. **Symptom**: run it (a few times). What happens? Quote the error.
2. **Hypothesis**: what's wrong, and why does that cause this symptom?
3. **Fix**: smallest change that fixes the root cause (not a buffer or sleep that hides it).
4. **Verify**: run again, with `-race` too.
5. **Interview answer**: one or two sentences on how you'd prevent this class of bug.

| Drill | Scenario |
|---|---|
| d01_counter | 100 goroutines counting hits |
| d02_squares | collecting results from 10 goroutines |
| d03_producers | three producers feeding one consumer |
| d04_pipeline | two-stage pipeline |
| d05_wordcount | counting words across documents |
| d06_jobs | jobs that can fail |
| d07_inventory | a mutex-protected inventory |
| d08_search | taking the fastest of several replicas |

Some bugs crash loudly, some are silent, and one only shows up as a failed check. Tip: `go vet` catches one of them, but CoderPad may not have it.

## Build drills (`build/`), 30–45 min each

A problem statement at the top, a stub to fill in, and a `main` that prints PASS/FAIL. Don't edit the checks.

| Drill | Topic | Time |
|---|---|---|
| b01_cache | thread-safe cache with TTL | 30 min |
| b02_fetch_limit | concurrent fetch with a concurrency limit, ordered results | 35 min |
| b03_top_words | parsing, maps, sorting (no concurrency) | 25 min |
| b04_first_success | `select` + `context`: first success wins, cancel the rest | 45 min (stretch) |

Routine: clarify → plan out loud → simplest working version → run the checks → improve.
