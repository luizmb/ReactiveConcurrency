# ReactiveConcurrency

[![CI](https://github.com/luizmb/ReactiveConcurrency/actions/workflows/ci.yml/badge.svg)](https://github.com/luizmb/ReactiveConcurrency/actions/workflows/ci.yml)
![Platforms](https://img.shields.io/badge/platforms-macOS%20%7C%20iOS%20%7C%20Linux%20%7C%20Windows%20%7C%20Android-blue)
![Swift](https://img.shields.io/badge/Swift-6.3-orange)
![License](https://img.shields.io/badge/license-Apache--2.0-lightgrey)

A **cross-platform reactive framework** built on top of modern Swift Concurrency and the
compiler's `Sendable` thread-safety guarantees.

If you know Combine, you already know most of the surface — `Publisher`, `sink`, `map`,
`combineLatest`, subjects — but ReactiveConcurrency is **not** a Combine port. It takes a more
modern, pragmatic view:

- **Built on Swift Concurrency, not a bespoke runtime.** Every `Publisher` is an `AsyncSequence`
  under the hood. Delivery, cancellation, and backpressure come from structured concurrency, and
  thread-safety is enforced by the compiler (`Sendable` everywhere) rather than by convention.
- **Lazy across the board.** Nothing runs at construction. There are **no implicit side effects**
  when you build a pipeline — it only executes when you explicitly *run* it (`sink`, iterate
  `for await`, or `firstValue()`). The same publisher value can be run many times.
- **Pragmatic backpressure.** Combine's fine-grained, per-element demand (`request(.max(n))`) is
  rarely used and hard to reason about. ReactiveConcurrency drops it in favour of simple
  **cold/hot** semantics plus `AsyncStream` buffering — the model Swift Concurrency already gives
  you.
- **Typed errors, no `any Error`.** A `Publisher<Output, Failure>` carries its failure type, and
  the error is surfaced as a *value* (`Result`) at the iteration boundary, never as an untyped
  thrown error.
- **Founded on functional algebra.** `Publisher` is a lawful Functor / Applicative / Monad /
  Alternative, with **opt-in symbolic operators** (`<£>`, `>>-`, `<*>`, …) for those who want
  point-free composition — and ordinary methods for everyone else.
- **Genuinely cross-platform.** CI builds and runs the full test suite on **macOS, Linux, Android
  (emulator), and Windows**. No `Combine`, `UIKit`, or any Apple-only API in the library.

> Want the design rationale and the numbers behind "built on Swift Concurrency"? See
> [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for a head-to-head against Combine and raw
> `AsyncStream`, and for why synchronous reentrancy bugs can't happen in this model.

---

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Creating publishers](#creating-publishers)
- [Subjects (hot publishers)](#subjects-hot-publishers)
- [Consuming: running a publisher](#consuming-running-a-publisher)
- [Operators](#operators)
  - [Reading the marble diagrams](#reading-the-marble-diagrams)
  - [Transforming](#transforming)
  - [Filtering](#filtering)
  - [Combining](#combining)
  - [Time-based](#time-based)
  - [Error handling](#error-handling)
  - [Sharing & multicasting](#sharing--multicasting)
  - [Side effects & debugging](#side-effects--debugging)
  - [Reducing to a single value](#reducing-to-a-single-value)
- [Async / await & AsyncSequence interop](#async--await--asyncsequence-interop)
- [The functional core](#the-functional-core)
- [Diagnostics](#diagnostics)
- [Performance](#performance)
- [Platform support](#platform-support)
- [License](#license)

---

## Installation

Add the package to your `Package.swift`:

```swift
.package(url: "https://github.com/luizmb/ReactiveConcurrency.git", from: "0.5.0")
```

Then depend on the products you need:

```swift
.target(
    name: "MyFeature",
    dependencies: [
        .product(name: "ReactiveConcurrency", package: "ReactiveConcurrency"),
        // Optional — symbolic FP operators (<£>, >>-, <*>, …):
        .product(name: "ReactiveConcurrencyOperators", package: "ReactiveConcurrency"),
        // Optional — monad-transformer stacks over Publisher:
        .product(name: "ReactiveConcurrencyTransformers", package: "ReactiveConcurrency"),
    ]
)
```

Minimum platforms: macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1 — and Linux / Windows / Android
via the open-source Swift toolchain.

---

## Quick start

```swift
import ReactiveConcurrency

// Build a pipeline — nothing runs yet. This is just a value.
let pipeline = [1, 2, 3, 4, 5].publisher        // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }            // Publisher<Int, Never> → 2, 4
    .map { $0 * 10 }                            // Publisher<Int, Never> → 20, 40

// Run it. `sink` is the boundary where execution actually happens.
let cancellable = pipeline.sink { value in
    print(value)                                // 20, then 40
}

// …or consume it as an AsyncSequence:
for await value in pipeline.values {            // AsyncSequence<Int>
    print(value)                                // 20, then 40
}
```

---

## Core concepts

### A `Publisher` is a lazy value, not a running process

```swift
Publisher<Output: Sendable, Failure: Error>
```

A publisher is a recipe. Building one — and chaining operators onto it — performs no work and has
no side effects. Work begins only when you **run** it by subscribing (`sink`) or iterating
(`for await`). Because building is pure, the same publisher can be run repeatedly, and each run is
independent.

This is the opposite of Combine's `Future`, which starts immediately on creation. Here,
everything is cold by default.

### Cold vs hot — the whole backpressure story

There is no fine-grained demand protocol. Instead:

- **Cold** publishers (`just`, `sequence`, `future`, `map`-chains, …) produce their values fresh
  for *each* subscriber, driven by how fast that subscriber consumes. A slow `for await` loop
  naturally slows production — backpressure for free, courtesy of `AsyncStream`.
- **Hot** publishers (subjects) are *running already*; they broadcast to whoever is currently
  subscribed. A value sent with no subscribers is gone. Buffering between a hot source and a slow
  consumer is governed by `AsyncStream`'s buffering policy (see [`buffer`](#sharing--multicasting)).

That's it — no `Subscription.request(_:)`, no `Demand`.

### Typed errors as values

`Failure` is part of the type. A `Publisher<Int, MyError>` can only ever fail with `MyError`. When
you iterate a failable publisher you get the failure as a **value**, so iteration never throws an
untyped `any Error`:

```swift
for await result in failablePublisher.results {   // AsyncSequence<Result<Int, MyError>>
    switch result {
    case .success(let value): …
    case .failure(let error): …                   // error is MyError, statically
    }
}
```

When `Failure == Never`, use `.values` for a plain `AsyncSequence<Output>`.

### Cancellation

`sink` returns an `AnyCancellable`. Cancelling it (or letting it deinit) tears down the underlying
`Task` cooperatively. Cancellation never invokes your completion handler — matching Combine's
contract.

---

## Creating publishers

```swift
// Single value, then finish.
Publisher<Int, Never>.just(42)                       // → 42 |

// Finish immediately with no values.
Publisher<Int, Never>.empty()                        // → |

// Fail immediately.
Publisher<Int, MyError>.fail(.boom)                  // → X(boom)

// Every element of a Sequence, then finish.
[1, 2, 3].publisher                                  // Publisher<Int, Never> → 1 2 3 |
Publisher<Int, Never>.sequence(1...3)                // same

// A lazy async single-shot (cold — re-runs per subscription, unlike Combine's eager Future).
Publisher<Data, NetworkError>.future {              // async throws(NetworkError) -> Data
    try await fetch()
}

// A repeating timer (values are clock instants).
Publisher<C.Instant, Never>.timer(every: .seconds(1), clock: ContinuousClock())

// Bridge an existing AsyncStream.
someAsyncStream.eraseToPublisher()                   // Publisher<Element, Never>
```

---

## Subjects (hot publishers)

Subjects are the imperative entry point — push values in, fan them out to subscribers.

```swift
let subject = PassthroughSubject<Int, Never>()

let c = subject.eraseToPublisher().sink { print($0) }

subject.send(1)                 // prints 1
subject.send(2)                 // prints 2
subject.send(completion: .finished)
```

```swift
// Replays its current value to every new subscriber.
let current = CurrentValueSubject<Int, Never>(0)
current.value                    // 0  (synchronous read)
current.send(1)
current.value                    // 1
```

`CurrentValueSubject.value` updates synchronously; delivery to subscribers is asynchronous (a value
is buffered into each subscriber's stream and drained on its own task). Subjects deliver via
`AsyncStream` continuations and **never** call subscriber code synchronously, which is why
RxSwift-style reentrancy anomalies simply cannot occur here. Use `eraseToAnySubject()` for a
type-erased subject.

---

## Consuming: running a publisher

```swift
// 1. sink — the classic subscriber. Returns AnyCancellable.
let c = publisher.sink(
    receiveCompletion: { completion in … },   // .finished or .failure(Failure)
    receiveValue: { value in … }
)
// When Failure == Never, the value-only overload is available:
let c2 = neverFailing.sink { value in … }

// 2. AsyncSequence — iterate directly.
for await value in neverFailing.values { … }          // AsyncSequence<Output>
for await result in failable.results { … }            // AsyncSequence<Result<Output, Failure>>

// 3. Single shot — await the first value/result, then stop.
let first: Int?               = await neverFailing.firstValue()    // Output?
let firstR: Result<Int, E>?   = await failable.firstResult()       // Result<Output, Failure>?

// 4. assign — write each value to a property via key path.
let c3 = publisher.assign(to: \.title, on: viewModel)             // Root: AnyObject & Sendable
let c4 = publisher.assignOnMain(to: \.title, on: view)           // ordered, on the main actor
```

---

## Operators

### Reading the marble diagrams

```
time  ─────────────────▶
─1──2──3─|      values 1, 2, 3 then completion
─1──X           value 1 then failure
─1──2──▶        ongoing (no completion shown)
```

Each example annotates the value **and type** flowing out of every step.

---

### Transforming

#### `map` — transform each value

```
input:        ─1──2──3─|
.map {$0*10}: ─10─20─30─|
```
```swift
[1, 2, 3].publisher        // Publisher<Int, Never>      → 1, 2, 3
    .map { $0 * 10 }       // Publisher<Int, Never>      → 10, 20, 30
    .map { "#\($0)" }      // Publisher<String, Never>   → "#10", "#20", "#30"
```

#### `compactMap` — transform and drop `nil`

```
input:                ─"1"──"x"──"3"─|
.compactMap{Int($0)}: ─1─────────3───|
```
```swift
["1", "x", "3"].publisher          // Publisher<String, Never>
    .compactMap { Int($0) }        // Publisher<Int, Never>   → 1, 3
```

#### `scan` — running accumulation, emitting each step

```
input:        ─1──2──3──4─|
.scan(0,+):   ─1──3──6──10|
```
```swift
[1, 2, 3, 4].publisher             // Publisher<Int, Never>
    .scan(0) { acc, x in acc + x } // Publisher<Int, Never>   → 1, 3, 6, 10
```

#### `removeDuplicates` — drop consecutive equal values

```
input:               ─1──1──2──2──1─|
.removeDuplicates(): ─1─────2─────1─|
```
```swift
[1, 1, 2, 2, 1].publisher          // Publisher<Int, Never>
    .removeDuplicates()            // Publisher<Int, Never>   → 1, 2, 1
    // or .removeDuplicates(by: { $0.id == $1.id })
```

#### `collect` — buffer into arrays

```
input:        ─1──2──3──4─|
.collect(2):  ────[1,2]──[3,4]|
.collect():   ───────────[1,2,3,4]|
```
```swift
[1, 2, 3, 4].publisher             // Publisher<Int, Never>
    .collect(2)                    // Publisher<[Int], Never>  → [1,2], [3,4]

[1, 2, 3, 4].publisher
    .collect()                     // Publisher<[Int], Never>  → [1,2,3,4]  (on completion)
```

---

### Filtering

#### `filter` — keep values matching a predicate

```
input:                       ─1──2──3──4─|
.filter{$0.isMultiple(of:2)}:────2─────4─|
```
```swift
[1, 2, 3, 4].publisher                    // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }      // Publisher<Int, Never>   → 2, 4
```

#### `prefix` / `first` / `last` / `output`

```
input:        ─1──2──3──4──5─|
.prefix(2):   ─1──2|
.first():     ─1|
.last():      ──────────────5|     (waits for completion)
.output(at:2):──────3|
```
```swift
[1, 2, 3, 4, 5].publisher          // Publisher<Int, Never>
    .prefix(2)                     // Publisher<Int, Never>   → 1, 2 then finish
[1, 2, 3, 4, 5].publisher.first()  // → 1 then finish
[1, 2, 3, 4, 5].publisher.last()   // → 5 (after upstream finishes)
[1, 2, 3, 4, 5].publisher.output(at: 2)        // → 3
[1, 2, 3, 4, 5].publisher.output(in: 1..<3)    // → 2, 3
```

#### `replaceNil` / `replaceEmpty` / `ignoreOutput`

```swift
[1, nil, 3].publisher              // Publisher<Int?, Never>
    .replaceNil(with: 0)           // Publisher<Int, Never>   → 1, 0, 3

Publisher<Int, Never>.empty()
    .replaceEmpty(with: -1)        // Publisher<Int, Never>   → -1

[1, 2, 3].publisher
    .ignoreOutput()                // Publisher<Int, Never>   → (no values) |
```

---

### Combining

#### `merge` — interleave values from several publishers of the same type

```
a:            ─1────3────▶
b:            ───2────4───▶
a.merge(with:b):─1─2──3─4──▶
```
```swift
let a = [1, 3].publisher           // Publisher<Int, Never>
let b = [2, 4].publisher           // Publisher<Int, Never>
a.merge(with: b)                   // Publisher<Int, Never>   → 1, 2, 3, 4 (interleaved)
Publisher.merge([a, b])            // same, from an array
```

#### `zip` — pair values index-by-index; finishes with the shortest

```
a:          ─1───2───3───4─|
b:          ──"a"──"b"──"c"|
a.zip(b):   ──(1,a)(2,b)(3,c)|
```
```swift
let ints = [1, 2, 3, 4].publisher          // Publisher<Int, Never>
let strs = ["a", "b", "c"].publisher       // Publisher<String, Never>
ints.zip(strs)                             // Publisher<(Int, String), Never> → (1,"a"),(2,"b"),(3,"c")
ints.zip(strs) { "\($0)\($1)" }            // Publisher<String, Never>        → "1a","2b","3c"
```

#### `combineLatest` — re-emit whenever *any* source emits, pairing latest values

```
a:                 ─1─────2──────────▶
b:                 ──────"a"────"b"───▶
a.combineLatest(b):──────(1,a)(2,a)(2,b)▶
```
```swift
let a = PassthroughSubject<Int, Never>()
let b = PassthroughSubject<String, Never>()
a.eraseToPublisher()
    .combineLatest(b.eraseToPublisher())   // Publisher<(Int, String), Never>
    .map { "\($0)\($1)" }                  // Publisher<String, Never>
// a=1, b="a" → "1a";  a=2 → "2a";  b="b" → "2b"
```

#### `flatMap` — map each value to a publisher and flatten

```
input:              ─1───────2────────▶
1→(10,11) 2→(20,21):─10─11───20─21─────▶
```
```swift
[1, 2].publisher                                       // Publisher<Int, Never>
    .flatMap { n in [n*10, n*10+1].publisher }         // Publisher<Int, Never> → 10,11,20,21
    // Bound the concurrency of inner publishers:
    .flatMap(maxPublishers: 1) { … }                   // sequential (one inner at a time)
```

#### `switchToLatest` — flatten a publisher-of-publishers, cancelling the previous inner

```swift
// Publisher<Publisher<T, F>, F> → Publisher<T, F>
// Each new inner publisher cancels the previous one — classic "search-as-you-type" shape.
queries
    .map { query in search(query) }     // Publisher<Publisher<Result, F>, F>
    .switchToLatest()                   // Publisher<Result, F>
```

#### `prepend` / `append`

```
input:            ─2──3─|
.prepend(0,1):    ─0─1─2──3─|
.append(4,5):     ─2──3─4─5─|
```
```swift
[2, 3].publisher.prepend(0, 1)     // Publisher<Int, Never>   → 0, 1, 2, 3
[2, 3].publisher.append(4, 5)      // Publisher<Int, Never>   → 2, 3, 4, 5
```

---

### Time-based

Time operators take any `Clock` (`ContinuousClock`, or Hourglass's `ImmediateClock` / `TestClock`
for deterministic tests). They are backed by [Hourglass](https://github.com/luizmb/Hourglass).

#### `delay` — shift everything later by an interval

```
input:                  ─1──2──3─|
.delay(for:.seconds(1)):──── 1──2──3─|
```
```swift
publisher.delay(for: .seconds(1), clock: ContinuousClock())   // Publisher<Output, Failure>
```

#### `debounce` — emit only after a quiet period (timer resets on each value)

```
input:                     ─1─2─3────────4─|
.debounce(.seconds(1)):    ──────────3──────4|
```
```swift
searchText                                          // Publisher<String, Never>
    .debounce(for: .milliseconds(300), clock: ContinuousClock())
```

#### `throttle` — emit at most once per window (leading or latest)

```
input:                       ─1─2─3──4─5─6─|
.throttle(win,latest:false): ─1──────4─────|   (first in each window)
.throttle(win,latest:true):  ────3──────6───|   (most recent in each window)
```
```swift
publisher.throttle(for: .seconds(1), clock: ContinuousClock(), latest: true)
```

#### `timeout` — fail if no value arrives within an interval

```
input:                       ─1──────────────▶   (silence)
.timeout(.seconds(2),.boom): ─1────────X(boom)
```
```swift
publisher.timeout(.seconds(2), clock: ContinuousClock(), error: MyError.timedOut)
```

#### `collect(every:)` / `measureInterval`

```swift
events.collect(every: .seconds(1), clock: clock)               // Publisher<[Event], F> — windowed
events.collect(every: .seconds(1), orCount: 50, clock: clock)  // flush on time OR count
events.measureInterval(using: clock)                           // Publisher<Duration, F> — gaps between values
```

---

### Error handling

#### `catch` / `tryCatch` — recover from failure with another publisher

```
input:                       ─1──X(boom)
.catch { _ in just(99) }:    ─1──99|
```
```swift
let failing = Publisher<Int, MyError> { c in c.yield(1); c.fail(.boom) }
failing
    .catch { error in Publisher<Int, Never>.just(99) }   // Publisher<Int, Never>  → 1, 99
// tryCatch lets the recovery itself fail with a (possibly different) typed error:
failing.tryCatch { _ throws(OtherError) in recovery }    // Publisher<Int, OtherError>
```

#### `retry` — resubscribe on failure up to N times

```
attempt 1:  ─1──X
attempt 2:  ─1──X
attempt 3:  ─1──2──3─|
.retry(2):  ─1──1──1──2──3─|
```
```swift
flaky.retry(2)                     // Publisher<Int, MyError> — up to 3 attempts total
```

#### `replaceError` / `mapError` / `setFailureType` / `assertNoFailure`

```swift
failing.replaceError(with: 0)              // Publisher<Int, Never>   — failure → 0 then finish
failing.mapError { OtherError($0) }        // Publisher<Int, OtherError>
neverFailing.setFailureType(to: E.self)    // Publisher<Int, E>       — adapt Never → E
failing.assertNoFailure()                  // Publisher<Int, Never>   — traps if it ever fails
```

---

### Sharing & multicasting

By default each subscription re-runs a cold publisher. To share *one* execution among many
subscribers:

```swift
let shared = expensive.share()             // multicast via a PassthroughSubject, auto-connect

// Explicit control:
let connectable = expensive.makeConnectable()   // ConnectablePublisher — won't start until…
let c1 = connectable.sink { … }
let c2 = connectable.sink { … }
let conn = connectable.connect()                // …you connect; now both receive

// Multicast through a specific subject (e.g. CurrentValueSubject to replay latest):
let replayed = expensive.multicast { CurrentValueSubject<Int, Never>(0) }

// Bound buffering between a hot source and a slow consumer:
hot.buffer(size: 16, whenFull: .dropOldest)     // .dropOldest | .dropNewest
```

---

### Side effects & debugging

```swift
publisher
    .handleEvents(                 // inject side effects without changing the stream
        receiveSubscription: { … },
        receiveOutput: { value in … },
        receiveCompletion: { completion in … },
        receiveCancel: { … }
    )
    .print("pipeline")             // log every event with a prefix
```

---

### Reducing to a single value

These consume the whole stream and emit one summary value when the upstream finishes:

| Operator | Result type | Description |
|---|---|---|
| `reduce(_:_:)` | `Publisher<T, F>` | Fold all values into one |
| `count()` | `Publisher<Int, F>` | Number of values |
| `min()` / `max()` | `Publisher<Output, F>` | Extremes (also `by:` overloads) |
| `contains(_:)` / `contains(where:)` | `Publisher<Bool, F>` | Membership test |
| `allSatisfy(_:)` | `Publisher<Bool, F>` | Predicate over all values |

```swift
[1, 2, 3, 4].publisher.reduce(0, +)        // Publisher<Int, Never>   → 10
[1, 2, 3].publisher.contains(2)            // Publisher<Bool, Never>  → true
```

#### `try`-prefixed variants

Most filtering/transforming operators have a `try`-prefixed sibling whose closure can throw a
**typed** error, turning a `Publisher<_, Never>` into a `Publisher<_, E>`:

```swift
["1", "x"].publisher
    .tryMap { s throws(ParseError) in try parse(s) }   // Publisher<Int, ParseError>
```

Available: `tryMap`, `tryCompactMap`, `tryFilter`, `tryScan`, `tryReduce`, `tryFirst`, `tryLast`,
`tryDrop`, `tryPrefix`, `tryContains`, `tryAllSatisfy`, `tryRemoveDuplicates`, `tryCatch`. Each also
has a `Result`-returning form for when your transform already yields a `Result` instead of throwing.

---

## Async / await & AsyncSequence interop

ReactiveConcurrency is designed to live *inside* Swift Concurrency, not beside it.

```swift
// Publisher → AsyncSequence
for await value in publisher.values { … }        // when Failure == Never
for await result in publisher.results { … }       // any Failure (Result elements)

// Publisher → single awaited value
let value = await publisher.firstValue()          // Output?
let result = await publisher.firstResult()        // Result<Output, Failure>?

// AsyncStream → Publisher
stream.eraseToPublisher()                          // Publisher<Element, Never>
```

It also bridges the lazy primitives from [FP](https://github.com/luizmb/FP) — `DeferredTask`
(single async value) and `DeferredStream` (lazy async sequence):

```swift
deferredStream.eraseToPublisher()                  // DeferredStream<A>            → Publisher<A, Never>
deferredResultStream.eraseToThrowingPublisher()    // DeferredStream<Result<A,E>>  → Publisher<A, E>
publisher.results                                  // Publisher<A, E>              → DeferredStream<Result<A,E>>

deferredTask.eraseToPublisher()                    // DeferredTask<A>              → Publisher<A, Never>
publisher.firstValueTask()                         // Publisher<A, Never>          → DeferredTask<A?>  (lazy)
```

For an environment-dependent effect, compose `Reader` with `Publisher` — the `ReaderTPublisher`
transformer (`Reader<Env, Publisher<A, E>>`) provides `mapT` / `flatMapT` and the symbolic operators.

---

## The functional core

`Publisher` is a lawful **Functor**, **Applicative**, **Monad**, and **Alternative**. You can use
plain methods (`map`, `flatMap`, …) or, by importing `ReactiveConcurrencyOperators`, **opt in** to
symbolic operators for point-free composition:

| Operator | Meaning | Method equivalent |
|---|---|---|
| `<£>` | functor map (function on the left) | `map` |
| `<&>` | functor map (publisher on the left) | `map` |
| `£>` / `<£` | replace every value with a constant | `replace(_:)` / `void` |
| `<*>` | applicative apply | `applyPublisher` |
| `*>` / `<*` | sequence, keep right / keep left | `seqRight` / `seqLeft` |
| `>>-` / `-<<` | monadic bind (flatMap) — container / function on the left | `flatMap` |
| `>=>` | Kleisli composition of `A -> Publisher<B>` | `Publisher.kleisli` |
| `<\|>` | alternative (here: concatenation) | `alt` |

```swift
import ReactiveConcurrency
import ReactiveConcurrencyOperators

let doubled = { $0 * 2 } <£> [1, 2, 3].publisher          // Publisher<Int, Never> → 2, 4, 6
let chained = [1, 2].publisher >>- { n in [n, -n].publisher }   // → 1, -1, 2, -2
```

Static FP constructors mirror the algebra too: `Publisher.pure(_:)` (a.k.a. `just`),
`Publisher.fmap`, `Publisher.flatMap`, `Publisher.join`, `Publisher.zip`, `Publisher.kleisli`,
`Publisher.alt`.

### Monad transformers

`ReactiveConcurrencyTransformers` provides transformer stacks layering an inner effect over
`Publisher` — `PublisherT{Either, Array, Optional, Result, Validation}` and
`{Reader, Writer, Stateful}TPublisher` — with `map`/`apply`/`flatMap` (`…T…`) free functions and
their own symbolic operators (`<£^>`, `<&^>`, …). `Validation` accumulates errors via its
applicative; `Stateful` is applicative-only (state can't thread across `await`). This is a niche
but powerful layer — reach for it when you're composing effects, not for everyday UI streams.

---

## Diagnostics

An opt-in diagnostics facility catches common lifecycle misuse — e.g. sending a value or completion
to a subject that has already completed. It is **on in DEBUG, off in release**, and routes to a
configurable handler (stderr by default):

```swift
Diagnostics.isEnabled = true
Diagnostics.setHandler { message in logger.warning("\(message)") }
```

> There is no reentrancy detector because reentrancy can't happen: subjects deliver asynchronously
> via `AsyncStream`, so a subscriber can never re-enter `send` mid-delivery. See
> [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

---

## Performance

Being async-native has a cost and a payoff, both measured in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md):

- Every element crosses a real `AsyncStream` suspension, so per-element throughput is roughly an
  order of magnitude below Combine's synchronous in-process delivery (~0.6 µs vs ~7 µs/element for a
  simple chain). This library is sized for **event-rate streams** — UI events, network responses,
  timers, subjects — not million-element-per-second data crunching.
- In exchange, delivery is asynchronous and structured: **reentrancy anomalies and recursive-`send`
  stack overflows are structurally impossible**, cancellation and backpressure come from the
  language, and the same code runs on platforms where Combine doesn't exist.

---

## Platform support

| Platform | Build | Test |
|---|:---:|:---:|
| macOS / iOS / tvOS / watchOS / visionOS | ✅ | ✅ |
| Linux | ✅ | ✅ |
| Android (emulator) | ✅ | ✅ |
| Windows | ✅ | ✅ |

All four are exercised on every PR and gated in the release process. The realistic Android use case
is a **Swift shared core** consumed from a Kotlin/Compose app over JNI — the reactive/business
layer in Swift, the UI native to the platform.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
