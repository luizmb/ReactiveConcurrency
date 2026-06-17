# Benchmarks

Lightweight, dependency-free throughput benchmarks for `ReactiveConcurrency`.

```bash
swift run -c release Benchmarks
```

Wall-clock throughput only — no allocation profiling (that would need
[package-benchmark](https://github.com/ordo-one/package-benchmark) + jemalloc, which complicates
the Linux/Windows/Android story). This is enough to characterize per-operator overhead and catch
regressions; allocation profiling is a possible future upgrade.

## Representative results

Apple M-series, `-c release`, 1,000,000 elements (numbers vary run-to-run — read the *ratios*,
not the absolutes):

| Scenario | Throughput | ≈ per element |
|---|---:|---:|
| baseline `AsyncStream` (no operators) | ~0.5 M/s | ~2 µs |
| `Publisher.sequence.values` | ~0.12 M/s | ~8 µs |
| `map ×1` | ~0.12 M/s | ~8 µs |
| `map ×10` | ~0.07 M/s | ~14 µs |
| `filter` | ~0.18 M/s | ~6 µs |
| `scan` | ~0.11 M/s | ~9 µs |
| `flatMap` (1 inner each) | ~0.08 M/s | ~13 µs |
| `merge ×4` | ~0.09 M/s | ~11 µs |
| `zip2` | ~0.08 M/s | ~12 µs |
| `PassthroughSubject` send+drain | ~0.08 M/s | ~12 µs |

## What this tells us

1. **Every element crosses at least one `async` suspension.** `AsyncStream.next()` is `async`, so
   even the raw baseline is ~2 µs/element (~0.5 M/s). This is the floor of an
   `AsyncStream`-per-stage design, not a defect.
2. **Each stage adds another `AsyncStream` + `Task` hop.** `Publisher.sequence.values` is ~4× the
   raw baseline (the value channel is a second stream over the `Result` stream), and every operator
   adds roughly another ~0.6 µs/element (`map ×1` → `map ×10`).
3. **`zip` / `merge` / `flatMap` are heavier** — they coordinate via `withTaskGroup`.

## Cross-library comparison

The same identity (`source → sink`) and 3× `map` workloads run against pure `AsyncStream`,
`ReactiveConcurrency`, and Combine. All three sum 1,000,000 elements through a single consumer and
cross-check the same checksum, so they do identical work. The Combine rows are compiled and run only
where `canImport(Combine)` (Apple platforms); pure `AsyncStream` and `ReactiveConcurrency` run
everywhere.

Apple M-series, `-c release`, 1,000,000 elements (single run — read the *ratios*):

| Scenario | pure `AsyncStream` | `ReactiveConcurrency` | Combine |
|---|---:|---:|---:|
| identity (source → sink) | ~5.2 µs/elem | ~7.5 µs/elem | **~0.63 µs/elem** |
| 3× `map` | ~6.9 µs/elem | ~10.9 µs/elem | **~0.68 µs/elem** |

(These are concurrent producer→consumer figures; the ~2 µs `AsyncStream` baseline in the first table
is a single-task drain of a pre-filled buffer — cheaper because nothing crosses tasks.)

### How to read it

- **Combine is ~12–16× faster than `ReactiveConcurrency`, and ~8× faster than pure `AsyncStream`.**
  Combine delivers *synchronously* — `send` walks the operator chain on the current call stack with
  no executor hop — so it has no per-element scheduling cost and is **nearly flat across operators**
  (three maps add only ~45 ns/elem; it fuses the chain into one call stack).
- **`ReactiveConcurrency` sits ~1.5× above the concurrent `AsyncStream` floor.** That gap is the
  `Result`-wrapping (a `Publisher<Output, Failure>` is a `DeferredStream<Result<Output, Failure>>`),
  the source-production task, and the sink task. Each operator adds another `AsyncStream` + `Task`
  hop (~1.3 µs/elem here), where Combine adds essentially nothing.
- **RxSwift is not measured** (not a dependency), but it belongs to the same *synchronous observer*
  family as Combine — direct calls, no executor hop — so it lands in the same hundreds-of-ns to
  low-µs/element range, an order of magnitude faster than any async-per-element model.

The cost is the direct consequence of being **async-native and cross-platform**: every element
crosses a real `AsyncStream` suspension. The flip side is structural — delivery is asynchronous, so
synchronous reentrancy (RxSwift's "reentrancy anomaly") and recursive-`send` stack overflows simply
cannot occur, cancellation and backpressure come from structured concurrency, and the same code runs
on Linux/Windows/Android where Combine does not exist. See `Diagnostics` for the reentrancy note.

### Implication

This library is built for **event-rate streams** — UI events, network responses, timers,
subjects — where elements arrive at human/IO rates and the µs-per-element cost is irrelevant. It is
**not** built for high-throughput element crunching (millions of items/second); for hot data paths,
process arrays/sequences synchronously and lift the result into a publisher at the boundary.

If synchronous operator fusion (collapsing a chain into one stage, like Combine does) becomes a
goal, it would be a substantial architectural change — the benchmark target exists to measure any
such work against this baseline.
