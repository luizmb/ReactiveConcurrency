# Core Concepts

The mental model behind ReactiveConcurrency: pipelines are pure, lazy values, and execution happens only at the boundary where you run them.

## Overview

Everything in ReactiveConcurrency rests on a single idea: a ``Publisher`` is a *value*, not a
running process. This page is the conceptual hub — read it once, and the operator articles
(<doc:TransformingValues>, <doc:FilteringValues>, <doc:CombiningPublishers>,
<doc:ControllingTiming>, <doc:HandlingErrors>) all follow from it.

### A pipeline is a pure, lazy value

Building a publisher — and chaining operators onto it — performs no work and has no side effects.
It is a *recipe*. Work begins only when you **run** the recipe by subscribing (`sink`) or iterating
(`for await`).

```swift
import ReactiveConcurrency

// Nothing runs here. `pipeline` is just a value describing future work.
let pipeline = [1, 2, 3, 4, 5].publisher     // Publisher<Int, Never>
    .filter { $0.isMultiple(of: 2) }         // → 2, 4
    .map { $0 * 10 }                         // → 20, 40

// The boundary: execution actually happens here.
let cancellable = pipeline.sink { print($0) }   // 20, then 40
```

Because construction is pure, the *same* publisher value can be run many times, and each run is
independent. This is the opposite of Combine's `Future`, which starts immediately on creation —
here everything is cold by default.

### The sink / iterate boundary

There are three ways to cross the boundary from "value" to "execution":

```swift
// 1. sink — the classic subscriber. Returns AnyCancellable.
let c = pipeline.sink { value in print(value) }

// 2. AsyncSequence — iterate directly with for await.
for await value in pipeline.values { print(value) }   // Failure == Never
for await result in failable.results { … }            // Result<Output, Failure> elements

// 3. Single shot — await just the first value/result, then stop.
let first = await pipeline.firstValue()               // Output?
```

Inside a pipeline you never `await`; you compose operators. The system boundary (`sink`, `for
await`, `firstValue()`) is the only place execution occurs.

### Cold vs hot — the whole backpressure story

There is no fine-grained demand protocol (no `request(.max(n))`, no `Demand`). Instead:

- **Cold** publishers (`just`, `sequence`, `future`, and every `map`/`filter` chain) produce their
  values fresh for *each* subscriber.
- **Hot** publishers (subjects — see `PassthroughSubject` / `CurrentValueSubject`) are already
  running and broadcast to whoever is subscribed *now*. A value sent with no subscribers is gone.

```swift
let subject = PassthroughSubject<Int, Never>()
let c = subject.eraseToPublisher().sink { print($0) }
subject.send(1)   // prints 1
subject.send(2)   // prints 2
```

### Interior buffers are unbounded

Each operator in a chain is backed by an `.unbounded` `AsyncStream` buffer. There is **no**
automatic backpressure propagating up the chain: a slow consumer at the end does not throttle a
cold source, so a source like `sequence` can flood its buffer on subscription. Real bounding is
opt-in via `buffer(size:whenFull:)`:

```swift
hotSource.buffer(size: 16, whenFull: .dropOldest)   // .dropOldest | .dropNewest
```

### Typed errors as values

`Failure` is part of the type. A `Publisher<Int, MyError>` can only fail with `MyError`, and the
failure surfaces as a **value** at the iteration boundary — iteration never throws an untyped
`any Error`. See <doc:HandlingErrors> for the full story.

```swift
for await result in failable.results {   // AsyncSequence<Result<Int, MyError>>
    switch result {
    case let .success(value): …
    case let .failure(error): …          // error is MyError, statically
    }
}
```

### Cancellation is cooperative

`sink` returns an `AnyCancellable`. Cancelling it — or simply letting it deinit — tears down the
underlying `Task` cooperatively. Cancellation never invokes your completion handler (matching
Combine's contract), but it is *cooperative, not synchronous*: `cancel()` stops future deliveries
promptly, yet an already-in-flight callback is not interrupted mid-flight.

```swift
var cancellable: AnyCancellable? = pipeline.sink { … }
cancellable = nil   // dropping the AnyCancellable cancels the subscription
```

## Topics

### Related articles
- <doc:TransformingValues>
- <doc:FilteringValues>
- <doc:CombiningPublishers>
- <doc:ControllingTiming>
- <doc:HandlingErrors>
