# Combining Publishers

Bring several streams together — interleave, pair, flatten, and concatenate.

## Overview

Combining operators take more than one ``Publisher`` and weave them into a single stream. Choosing
the right one comes down to *how* you want values from the sources related in time. All of them
preserve the pure, lazy model from <doc:CoreConcepts>: nothing runs until you subscribe.

### merge, combineLatest, zip — the three shapes

These three are the ones people confuse, so hold them side by side:

- **`merge`** — same-typed sources interleaved in arrival order. No pairing; every value passes
  through as-is.
- **`combineLatest`** — re-emits whenever *any* source emits, pairing each new value with the
  *latest* value from the others. Needs every source to have emitted at least once.
- **`zip`** — pairs values *index-by-index* (positional, ZipList-style) and finishes with the
  shortest source. No value is ever paired twice.

```swift
// merge — interleave same-typed streams
let a = [1, 3].publisher               // Publisher<Int, Never>
let b = [2, 4].publisher               // Publisher<Int, Never>
a.merge(with: b)                       // Publisher<Int, Never>   → 1, 2, 3, 4 (interleaved)
a.merge(with: b, [5, 6].publisher)     // 2- and 3-arity overloads exist
Publisher.merge([a, b])                // …or an array of sources
```

```swift
// combineLatest — pair with the latest of each
let ints = PassthroughSubject<Int, Never>()
let strs = PassthroughSubject<String, Never>()
ints.eraseToPublisher()
    .combineLatest(strs.eraseToPublisher())   // Publisher<(Int, String), Never>
    .map { "\($0)\($1)" }                     // Publisher<String, Never>
// ints=1, strs="a" → "1a";  ints=2 → "2a";  strs="b" → "2b"
```

```swift
// zip — positional pairing, truncates to the shortest
let nums = [1, 2, 3, 4].publisher             // Publisher<Int, Never>
let tags = ["a", "b", "c"].publisher          // Publisher<String, Never>
nums.zip(tags)                                // Publisher<(Int, String), Never> → (1,"a"),(2,"b"),(3,"c")
nums.zip(tags) { "\($0)\($1)" }               // Publisher<String, Never>        → "1a","2b","3c"
```

Both `combineLatest` and `zip` offer 2-, 3-, and 4-source arities that yield typed tuples, plus a
transform-closure overload that maps the tuple in place. `zip`'s positional, truncating product is
the framework's *zippy* applicative — reach for `flatMap` when you want the monad-consistent
cartesian product instead.

### flatMap and maxPublishers

`flatMap` maps each value to a *new* publisher and flattens the results. By default all inner
publishers run concurrently. Passing `maxPublishers:` bounds how many run at once — `maxPublishers:
1` makes it strictly sequential, which is the framework's real backpressure lever (upstream
consumption pauses while all slots are busy).

```swift
[1, 2].publisher                                       // Publisher<Int, Never>
    .flatMap { n in [n * 10, n * 10 + 1].publisher }   // → 10, 11, 20, 21 (concurrent)

requests.publisher
    .flatMap(maxPublishers: 1) { req in fetch(req) }   // one inner publisher at a time
```

### switchToLatest — cancel the previous inner

When you have a `Publisher` of `Publisher`s, `switchToLatest()` flattens it but *cancels* the
previous inner whenever a new one arrives. This is the classic search-as-you-type shape.

```swift
queries
    .map { query in search(query) }     // Publisher<Publisher<Result, F>, F>
    .switchToLatest()                   // Publisher<Result, F> — only the newest search survives
```

### prepend and append

`prepend`/`append` splice fixed values (or another publisher) before/after the stream.

```swift
[2, 3].publisher.prepend(0, 1)     // Publisher<Int, Never>   → 0, 1, 2, 3
[2, 3].publisher.append(4, 5)      // Publisher<Int, Never>   → 2, 3, 4, 5
[2, 3].publisher.prepend([1].publisher)   // publisher overloads too
```

For error propagation across combined sources, see <doc:HandlingErrors>; for time-aware combining
like `debounce` upstream of `combineLatest`, see <doc:ControllingTiming>.

## Topics

### Combining
- ``Publisher/combineLatest(_:)``
- ``Publisher/zip(_:)``
- ``Publisher/switchToLatest()``
