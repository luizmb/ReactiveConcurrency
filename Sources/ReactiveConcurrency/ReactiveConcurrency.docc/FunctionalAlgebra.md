# Functional Algebra

`Publisher`, `DeferredTask`, and `DeferredStream` as lawful Functors, Monads, and Alternatives — and the symbolic operators for composing them.

## Overview

The three effect types share one algebra on purpose. Each is a lawful **Functor** (`map`),
**Monad** (`flatMap`/`join`/Kleisli), and — for the streamy ones — a **(concat) Alternative**
(`alt`/`empty`). Learn the vocabulary once and it transfers across all three, and across the monad
transformers built on them (see <doc:MonadTransformers>).

```swift
import ReactiveConcurrency

let doubled = [1, 2, 3].publisher.map { $0 * 2 }                 // Functor
let chained = [1, 2].publisher.flatMap { n in [n, -n].publisher } // Monad → 1,-1,2,-2
let either  = Publisher<Int, E>.alt(primary, fallback)           // Alternative (concat)
```

### `<*>` is `ap`, derived from the monad

The FP algebra on ``Publisher`` and ``DeferredStream`` binds with **ordered concat**: each inner
stream completes before the next starts, so results are deterministic and the monad laws hold. Their
applicative is `ap`, derived from that bind: `fs <*> xs` applies each function, in order, to every
value (the cartesian product). So `<*>` always agrees with `>>-`, and the applicative laws hold for
streams of any length.

```swift
let xs = [1, 2, 3].publisher

// `ap`: each left value against every right value, in order.
let pairs = liftA2Publisher { (x: Int, s: String) in (x, s) }(xs, ["a", "b"].publisher)
// → (1,"a"),(1,"b"),(2,"a"),(2,"b"),(3,"a"),(3,"b")

// Positional pairing is `zip`: it truncates to the shorter side.
let zipped = xs.zip(["a", "b"].publisher)      // → (1,"a"), (2,"b")   — the "3" is dropped
```

The `flatMap(_:)` *method* on ``Publisher`` keeps Combine's semantics: it merges, running inner
publishers concurrently and emitting in arrival order. `>>-`, the static `Publisher.flatMap(_:)`,
`join` and `kleisli` are the concat bind.

``DeferredTask`` holds a single value, so its `apply` is simply sequential: left, then right.

### The symbolic vocabulary

`import ReactiveConcurrencyOperators` to opt into point-free operators. They are the same symbols
FP uses everywhere, so pipelines read uniformly. Method equivalents always exist.

| Operator | Meaning | Method |
|---|---|---|
| `<£>` / `<&>` | functor map — function left / container left | `map` |
| `£>` / `<£` | replace every value with a constant | `replace(_:)` |
| `<*>` | applicative apply (`ap`: cartesian, in order) | `applyPublisher` |
| `*>` / `<*` | sequence, keep right / keep left | `seqRight` / `seqLeft` |
| `>>-` / `-<<` | monadic bind (ordered concat) — container left / function left | static `flatMap` |
| `>=>` / `<=<` | Kleisli composition — left-to-right / right-to-left | `kleisli` / `kleisliBack` |
| `<\|>` | alternative (here: concatenation) | `alt` |

```swift
import ReactiveConcurrency
import ReactiveConcurrencyOperators

let a = { $0 * 2 } <£> [1, 2, 3].publisher                   // → 2, 4, 6
let b = [1, 2].publisher >>- { n in [n, -n].publisher }       // → 1, -1, 2, -2
let c = primary <|> fallback                                  // concat: all of primary, then fallback
let pipeline = (getUser >=> loadProfile)(userID)              // Kleisli: A -> Publisher<C, E>
```

The exact same operators are defined for ``DeferredTask`` and ``DeferredStream``, so an effect
written point-free reads identically whichever type it flows through:

```swift
let task = { (n: Int) in n + 1 } <£> DeferredTask.pure(41)   // DeferredTask<Int> → 42
```

### Static constructors mirror the algebra

`Publisher.pure` (a.k.a. `just`), `.fmap`, `.flatMap`, `.join`, `.zip`, `.kleisli`, `.alt` are all
available as static/curried forms for building point-free pipelines.

For layering these effects over `Either`, `Result`, `Optional`, `Validation`, `Reader`, and friends,
see <doc:MonadTransformers>.

## Topics

### Related
- <doc:DeferredEffects>
- <doc:MonadTransformers>
