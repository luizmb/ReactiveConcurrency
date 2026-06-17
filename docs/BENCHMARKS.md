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

### Implication

This library is built for **event-rate streams** — UI events, network responses, timers,
subjects — where elements arrive at human/IO rates and the µs-per-element cost is irrelevant. It is
**not** built for high-throughput element crunching (millions of items/second); for hot data paths,
process arrays/sequences synchronously and lift the result into a publisher at the boundary.

If synchronous operator fusion (collapsing a chain into one stage, like Combine does) becomes a
goal, it would be a substantial architectural change — the benchmark target exists to measure any
such work against this baseline.
