# Sharing and Multicasting

Run one upstream execution and fan its values out to many subscribers.

## Overview

A ``Publisher`` is **cold by default**: each subscription re-runs the whole pipeline from the
source, independently. That is exactly what you want for a pure recipe — but it is wrong when the
work is *expensive* or *singular*: an HTTP request you only want to fire once, a timer whose ticks
every screen should agree on, a subject you want several views to observe. If two subscribers each
`sink` a cold publisher, the work happens twice.

```swift
let expensive = Publisher<Data, NetworkError>.future { try await fetch() }

let a = expensive.sink(receiveCompletion: { _ in }, receiveValue: { print("A", $0) })
let b = expensive.sink(receiveCompletion: { _ in }, receiveValue: { print("B", $0) })
// fetch() runs TWICE — once per subscription.
```

### `share()` — ref-counted multicast

``Publisher/share()`` turns a cold publisher into a *hot* one that is shared among current subscribers. The
upstream subscription starts when the **first** subscriber attaches and is torn down when the
**last** one cancels. It is reference-counted, so a later burst of subscribers restarts the
upstream cleanly.

```swift
let shared = expensive.share()             // Publisher<Data, NetworkError>

let a = shared.sink(receiveCompletion: { _ in }, receiveValue: { print("A", $0) })
let b = shared.sink(receiveCompletion: { _ in }, receiveValue: { print("B", $0) })
// fetch() runs ONCE; both A and B observe the same result.
```

This mirrors Combine's `share()`. Because it is hot, a value produced while nobody is subscribed is
gone — subscribe *before* the work starts producing if you need every element.

### Explicit control — `ConnectablePublisher`

When you need to line up several subscribers *before* the source starts, reach for
``Publisher/makeConnectable()``, which returns a ``ConnectablePublisher``. Nothing flows until you
call ``ConnectablePublisher/connect()``.

```swift
let connectable = expensive.makeConnectable()    // ConnectablePublisher<Data, NetworkError>
let c1 = connectable.eraseToPublisher().sink(receiveCompletion: { _ in }) { print("1", $0) }
let c2 = connectable.eraseToPublisher().sink(receiveCompletion: { _ in }) { print("2", $0) }
let connection = connectable.connect()           // AnyCancellable — now both receive
```

``ConnectablePublisher/autoconnect()`` connects on the first subscription and — unlike `share()` —
stays connected even as individual subscribers come and go, until the source completes.

### `multicast(subject:)` — choose the fan-out semantics

Multicasting through a specific ``Subject`` lets the subject drive delivery. Passing a
``CurrentValueSubject`` replays the latest value to late subscribers (a small "replay(1)"):

```swift
let replayed = expensive
    .multicast { CurrentValueSubject<Data, NetworkError>(.init()) }   // ConnectablePublisher
```

### `buffer(size:whenFull:)` — bound a hot source

Between a hot source and a slow consumer, ``Publisher/buffer(size:whenFull:)`` caps how many
undelivered elements are held, dropping per ``BufferStrategy`` when the buffer is full. It maps
onto `AsyncStream`'s buffering policy; the terminal failure is never dropped.

```swift
hotEvents.buffer(size: 16, whenFull: .dropOldest)   // keep the newest 16
hotEvents.buffer(size: 16, whenFull: .dropNewest)   // keep the oldest 16
```

This is the whole backpressure story here: there is no per-element `request(_:)` demand — see
<doc:CoreConcepts>. For the imperative hot sources you multicast through, see <doc:Subjects>.

## Topics

### Symbols
- ``Publisher/share()``
- ``Publisher/makeConnectable()``
- ``Publisher/multicast(subject:)``
- ``ConnectablePublisher``
- ``Publisher/buffer(size:whenFull:)``
- ``BufferStrategy``
