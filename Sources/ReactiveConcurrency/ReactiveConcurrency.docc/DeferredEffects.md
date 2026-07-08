# Deferred Effects

`DeferredTask` and `DeferredStream` — lazy async primitives, and when to reach for them instead of a `Publisher`.

## Overview

Swift's `Task` and `AsyncStream` are **eager**: constructing one starts the work. That makes them
awkward as values — you cannot build a description of an effect, pass it around, and decide later
whether (or how many times) to run it. ``DeferredTask`` and ``DeferredStream`` are the **lazy**
counterparts: they wrap the *recipe*, and nothing executes until you explicitly run them. The same
value can be run many times, each an independent computation — they are referentially transparent.

```swift
// Eager: this Task is already running.
let eager = Task { await fetch() }

// Lazy: just a value. fetch() has not been called.
let lazy = DeferredTask { await fetch() }
let a = await lazy.run()     // runs now
let b = await lazy.run()     // runs again, independently
```

### `DeferredTask<A>` — a single async value

``DeferredTask`` is the async equivalent of an IO monad: one deferred value. Build it with a
closure, run it with `run()`, or hand it to structured concurrency with `eraseToTask()`.

```swift
let user = DeferredTask { await api.getUser(id) }         // DeferredTask<User>

// Surface a typed throw as a Result rather than a thrown error:
let safe = DeferredTask.catching { try await api.getUser(id) }   // DeferredTask<Result<User, APIError>>
```

### The FP surface

Both types are full Functors, Applicatives, and Monads, with a shared vocabulary that mirrors
``Publisher`` so the three compose the same way (see <doc:FunctionalAlgebra>).

```swift
let name  = user.map(\.name)                              // DeferredTask<String>
let full  = user.flatMap { u in DeferredTask { await enrich(u) } }
let pure  = DeferredTask.pure(42)                         // DeferredTask<Int>

// Applicative product. DeferredTask's apply is SEQUENTIAL and lawful.
let paired = liftA2DeferredTask { (u: User, s: Settings) in Screen(u, s) }(loadUser, loadSettings)
let tupled = DeferredTask.zip(loadUser, loadSettings)     // DeferredTask<(User, Settings)>
let right  = loadUser.seqRight(loadSettings)              // keep the right
let left   = loadUser.seqLeft(loadSettings)               // keep the left
```

### `race` — first to finish wins

``race(_:_:)`` runs two tasks concurrently and returns whichever completes first, cancelling the
loser. It is the competitive counterpart to `zip`.

```swift
let fastest = race(fetchFromCache, fetchFromNetwork)      // DeferredTask<Data>
let data = await fastest.run()
```

### `DeferredStream<A>` — a lazy async sequence, with `alt`/`empty`

``DeferredStream`` is the multi-value sibling: a lazy `AsyncSequence` whose producer starts only at
first iteration. Its `flatMap` is a lawful sequential concatMap; its applicative (`zip`,
`applyDeferredStream`) is **zippy** — positional, truncating to the shortest side. It also forms a
monoid under `alt` (concatenation) with `empty` as identity.

```swift
let both = DeferredStream.alt(firstStream, secondStream)  // all of first, then all of second
let none = DeferredStream<Int>.empty()                    // identity: alt(empty, s) == s
let zipped = DeferredStream.zip(xs, ys)                   // DeferredStream<(X, Y)> — positional
```

### `sequence` / `traverse`

Turn a container of effects into an effect of a container. `DeferredTask` runs them **sequentially**
in order; `DeferredStream` is **zippy** (positional, truncating to the shortest input).

```swift
let all: DeferredTask<[User]> = traverseDeferredTask(ids) { id in DeferredTask { await api.getUser(id) } }
let collected: DeferredTask<[Int]> = sequenceDeferredTask([DeferredTask { 1 }, DeferredTask { 2 }])
```

### When to use which

- **``DeferredTask``** — one async result (a request, a computation). Prefer returning one from an
  effect and dispatching its result over `await`-chaining.
- **``DeferredStream``** — a lazy sequence with no failure channel and no reactive operators.
- **``Publisher``** — you need typed failures, subjects, time operators, sharing, or the full
  reactive operator set. Bridges both ways: <doc:BridgingAsyncSequence>.

## Topics

### Symbols
- ``DeferredTask``
- ``DeferredStream``
- ``race(_:_:)``
