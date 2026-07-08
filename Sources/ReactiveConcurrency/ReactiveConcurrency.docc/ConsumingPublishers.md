# Consuming Publishers

Run a pipeline: `sink`, `assign`, and the single-shot awaits are where execution happens.

## Overview

Building a ``Publisher`` does no work — it is a pure value (see <doc:CoreConcepts>). Something has
to **run** it, and that boundary is what this article is about. Every consumer here subscribes,
drives the underlying `AsyncStream`, and hands you an ``AnyCancellable`` (or an awaited value) back.

### `sink` — the classic subscriber

There are two overloads. The general one takes both handlers; when `Failure == Never` a
value-only overload is available.

```swift
let c = failable.sink(
    receiveCompletion: { completion in            // .finished or .failure(Failure)
        if case let .failure(error) = completion { report(error) }
    },
    receiveValue: { value in print(value) }
)

let c2 = neverFailing.sink { value in print(value) }   // Failure == Never
```

Cancelling the returned ``AnyCancellable`` (or letting it deinit) tears down the subscription
cooperatively. Cancellation never calls your completion handler — matching Combine.

### `assign(to:on:)` — write into a property

`assign(to:on:)` writes each value into a property via key path. **Unlike Combine,
`on:` is captured weakly** — `assign` will not keep the object alive, so retain it yourself;
delivery stops once the object is deallocated.

```swift
// Portable: Root promises its own thread-safety; the write happens on the subscription task.
let c = temperature.assign(to: \.currentTemp, on: sensorStore)   // Root: AnyObject & Sendable

// UI: main-actor-isolated object, written on the main actor, in order.
let c2 = await title.assignOnMain(to: \.text, on: label)
```

For a failable publisher, `assign` targets a `Result` property, writing `.success` per value and
`.failure` on failure.

### `handleEvents` / `print` / `breakpoint` — observe without consuming

These inject side effects but are not terminal — they return a publisher, so you still `sink` it.

```swift
pipeline
    .handleEvents(
        receiveOutput: { value in log("out", value) },
        receiveCompletion: { completion in log("done", completion) }
    )
    .print("pipeline")            // logs subscription/value/completion/cancel with a prefix
    .breakpointOnError()          // assertionFailure (DEBUG) if it ever fails
    .sink(receiveCompletion: { _ in }, receiveValue: { _ in })
```

### Single-shot — await the first event

When you want one value and done, `await` it directly. Each has a **lazy** `DeferredTask` form
(`…Task`) that composes into an effect without running yet — see <doc:DeferredEffects>.

```swift
let first: Int?                  = await neverFailing.firstValue()    // runs now
let firstR: Result<Int, E>?      = await failable.firstResult()

let task: DeferredTask<Int?>     = neverFailing.firstValueTask()      // lazy — nothing runs
let value = await task.run()                                          // …until here
```

`firstValue()` returns `nil` if the publisher finishes without emitting. To iterate every value
instead, use the `AsyncSequence` bridges in <doc:BridgingAsyncSequence>.

### `store(in:)` — keep cancellables alive

An ``AnyCancellable`` cancels on deinit, so hold onto it. `store(in:)` appends it
to a collection or `Set` you own.

```swift
var bag: Set<AnyCancellable> = []
pipeline.sink { print($0) }.store(in: &bag)
```

## Topics

### Symbols
- ``Publisher/sink(receiveCompletion:receiveValue:)``
- ``Publisher/handleEvents(receiveSubscription:receiveOutput:receiveCompletion:receiveCancel:)``
- ``Publisher/firstValue()``
- ``AnyCancellable``
