# Bridging AsyncSequence

Where `Publisher` meets Swift Concurrency — `.values`, `.results`, the driving `Continuation`, and the `DeferredTask` bridges.

## Overview

ReactiveConcurrency is built to live *inside* Swift Concurrency, not beside it. A ``Publisher`` is
internally a ``DeferredStream`` of `Result`s, so crossing between the reactive world and plain
`async`/`await` is cheap and lossless — the typed failure is preserved as a value, never surfaced as
an untyped `any Error`.

### Publisher → AsyncSequence

Two properties expose a publisher as an `AsyncSequence`:

- ``Publisher/values`` — a plain `AsyncSequence<Output>`, available only when `Failure == Never`.
- ``Publisher/results`` — an `AsyncSequence<Result<Output, Failure>>`, available for any `Failure`;
  the typed error arrives as a value.

```swift
for await value in neverFailing.values {         // Failure == Never
    print(value)
}

for await result in failable.results {           // any Failure
    switch result {
    case let .success(value): print(value)
    case let .failure(error): report(error)      // error is the static Failure type
    }
}
```

### Publisher → a single awaited value

For one value, `await` the first event directly, or take the lazy ``DeferredTask`` form to compose
an effect that has not run yet (see <doc:DeferredEffects>).

```swift
let v  = await neverFailing.firstValue()         // Output?
let r  = await failable.firstResult()            // Result<Output, Failure>?

let t: DeferredTask<Output?> = neverFailing.firstValueTask()   // lazy — nothing runs
```

### Building a Publisher — the `Continuation`

The `Publisher.init` body receives a ``Publisher/Continuation`` that mirrors
`AsyncStream.Continuation` but speaks the value/failure channels directly: `yield(_:)` emits a
success, `fail(_:)` emits a typed failure and seals the stream, and `finish()` completes.

```swift
let counter = Publisher<Int, Never> { continuation in
    continuation.yieldAll(0..<5)                 // sync sequence, checks cancellation per element
}
```

Two helpers are worth knowing. `yieldAll` forwards a sequence (sync or,
on newer OSes, an `AsyncSequence`) with cooperative cancellation between elements.
``Publisher/Continuation/suspendUntilCancelled()`` parks a callback-driven body until teardown —
the shape for wrapping a delegate or notification API:

```swift
let notifications = Publisher<Note, Never> { continuation in
    let token = center.observe { note in continuation.yield(note) }
    await continuation.suspendUntilCancelled()   // stay alive until the subscriber goes away
    center.remove(token)
}
```

### AsyncStream / DeferredStream → Publisher

An existing `AsyncStream` bridges with `eraseToPublisher()` (consumed once — it cannot restart). A
``DeferredStream`` bridges to a *cold, restartable* publisher; a stream of `Result` becomes a
failable publisher.

```swift
someAsyncStream.eraseToPublisher()               // Publisher<Element, Never> (single-use)

deferredStream.eraseToPublisher()                // DeferredStream<A>           → Publisher<A, Never>
deferredResultStream.eraseToThrowingPublisher()  // DeferredStream<Result<A,E>> → Publisher<A, E>
```

### DeferredTask ↔ Publisher

A ``DeferredTask`` becomes a cold single-value publisher; the reverse pulls the first value/result
back out as a lazy task.

```swift
deferredTask.eraseToPublisher()                  // DeferredTask<A>             → Publisher<A, Never>
deferredResultTask.eraseToThrowingPublisher()    // DeferredTask<Result<A,E>>   → Publisher<A, E>
publisher.firstValueTask()                       // Publisher<A, Never>         → DeferredTask<A?>
```

Together these let the reactive layer and structured concurrency hand work back and forth without
either owning the boundary — the system edge decides when to run. For the algebra these bridges
preserve, see <doc:FunctionalAlgebra>.

## Topics

### Symbols
- ``Publisher/values``
- ``Publisher/results``
- ``Publisher/Continuation``
- ``DeferredStream``
- ``DeferredTask``
