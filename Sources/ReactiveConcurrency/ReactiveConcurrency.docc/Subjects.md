# Subjects

Hot publishers you push values into imperatively and fan out to subscribers.

## Overview

Everything else in this library is *cold*: a ``Publisher`` is a lazy recipe that re-runs per
subscription (see <doc:CoreConcepts>). A **subject** is the exception — it is *hot*, already
running, and is the bridge from imperative code into a reactive pipeline. You `send` values in;
they fan out to whoever is currently subscribed.

The one rule of hot: **a value sent with no subscribers is lost.** There is no replay unless the
subject itself buffers (as ``CurrentValueSubject`` does for its latest value).

### `PassthroughSubject` — broadcast, no memory

``PassthroughSubject`` forwards each `send` to current subscribers and remembers nothing.

```swift
let subject = PassthroughSubject<Int, Never>()

let c = subject.eraseToPublisher().sink { print($0) }

subject.send(1)                        // prints 1
subject.send(2)                        // prints 2
subject.send(completion: .finished)    // seals the stream; later sends are ignored
```

### `CurrentValueSubject` — remembers the latest

``CurrentValueSubject`` holds a value, replays it to every new subscriber, and exposes it
synchronously through ``CurrentValueSubject/value``.

```swift
let current = CurrentValueSubject<Int, Never>(0)
current.value                    // 0  — synchronous read
current.send(1)
current.value                    // 1

let c = current.eraseToPublisher().sink { print($0) }   // immediately prints 1 (the replay)
current.send(2)                                          // prints 2
```

`value` updates synchronously; delivery to subscribers is **asynchronous** — the value is buffered
into each subscriber's `AsyncStream` and drained on its own task.

### Delivery is async — reentrancy can't happen

Both subjects deliver via `AsyncStream` continuations and **never** call subscriber code
synchronously from inside `send`. That is a deliberate difference from RxSwift/Combine subjects: a
subscriber cannot re-enter `send` mid-delivery, so the recursive-`send` reentrancy anomalies and
stack overflows those libraries guard against simply cannot occur here.

Note that `PassthroughSubject.send` *registration* is still synchronous with `sink`: a value sent
after `sink()` returns is guaranteed to reach that subscriber. It is the *callback* that is async.

### The `Subject` protocol and `AnySubject`

``Subject`` is the common contract — `send(_:)`, `send(completion:)`, and
``Subject/eraseToPublisher()``. Any subject can be type-erased with
``Subject/eraseToAnySubject()`` into an ``AnySubject`` value, which is handy for storing subjects of
mixed concrete types or passing them across boundaries.

```swift
func makeBus() -> AnySubject<Event, Never> {
    PassthroughSubject<Event, Never>().eraseToAnySubject()
}

let bus = makeBus()
let c = bus.eraseToPublisher().sink { handle($0) }
bus.send(.tapped)
```

Subjects are the natural source to multicast through — see <doc:SharingAndMulticasting>. Sending to
an already-completed subject is a no-op, and the opt-in `Diagnostics` facility flags it in DEBUG.

## Topics

### Symbols
- ``Subject``
- ``PassthroughSubject``
- ``CurrentValueSubject``
- ``AnySubject``
