---
name: bench-target-slow
description: `make bench` runs the full pubsub bench suite and can take minutes; for a quick pass use `-benchtime=200ms` or filter out the 10k-sub benches.
metadata:
  type: project
---

`make bench` runs `go test -bench=. -benchmem -run=^$ ./pubsub/...` with default benchtime (1s per bench, auto-scaled). Two benches dominate the wall time:

- `BenchmarkUltraLargeSubscribersSinglePublisher` — builds **10 000** subscriber set up, measures a single `Publish` against all of them.
- `BenchmarkHighLoadParallel` — same 10 000 subscribers with `b.SetParallelism(10)` × `GOMAXPROCS`.

Default `make bench` was killed by the harness timeout (>5 min) on the test/expand-coverage branch's Intel Core Ultra 5 125H. With `-benchtime=200ms` the whole suite finishes in **~7s** with all 16 benchmarks (10 small, 6 large/multi-sub).

**Why:** I tried to run `make bench` for the README refresh, the bench was killed by the harness; adding `-benchtime=200ms` cut it to 7s with stable numbers.

**How to apply:** For README-style "approximate performance" updates, run `go test -bench=. -benchmem -benchtime=200ms -run=^$ ./pubsub/...` directly — about 7s end-to-end, comparable to the old published numbers (same nanosecond-per-op order of magnitude). For the headline numbers in the README or for a release, run the full `make bench` on a fast workstation or in CI.

The README's bench table also needs the **CPU/architecture preamble updated** when run on a different host — the previous numbers were Windows / i7-10700F, the simplify-pass numbers are Linux / Ultra 5 125H.
