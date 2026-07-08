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

### The applicative is *zippy* — mind the identity law

Here is the one sharp edge. The `<*>` / `zip` product on ``Publisher`` and ``DeferredStream`` is a
**zippy Semigroupal** (ZipList-style): it pairs elements *positionally* and truncates at the shorter
side. That is the product you actually want from a stream — but it is **not** the cartesian
Applicative derived from the monad. Concretely, the identity law `pure(id) <*> v == v` **fails** for
`v` with more than one element, because `pure` yields a single element and the zip truncates `v` to
length 1.

```swift
let xs = [1, 2, 3].publisher

// Zippy: pairs positionally, truncates to the shorter side.
let zipped = xs.zip(["a", "b"].publisher)      // → (1,"a"), (2,"b")   — the "3" is dropped

// Want the cartesian, monad-consistent product? Use flatMap, not <*>.
let cartesian = xs.flatMap { x in ["a", "b"].publisher.map { (x, $0) } }
// → (1,"a"),(1,"b"),(2,"a"),(2,"b"),(3,"a"),(3,"b")
```

``DeferredTask`` is the exception: its single value means its `apply` is sequential and fully
lawful — no zippy caveat.

### The symbolic vocabulary

`import ReactiveConcurrencyOperators` to opt into point-free operators. They are the same symbols
FP uses everywhere, so pipelines read uniformly. Method equivalents always exist.

| Operator | Meaning | Method |
|---|---|---|
| `<£>` / `<&>` | functor map — function left / container left | `map` |
| `£>` / `<£` | replace every value with a constant | `replace(_:)` |
| `<*>` | applicative apply (zippy) | `applyPublisher` |
| `*>` / `<*` | sequence, keep right / keep left | `seqRight` / `seqLeft` |
| `>>-` / `-<<` | monadic bind — container left / function left | `flatMap` |
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
