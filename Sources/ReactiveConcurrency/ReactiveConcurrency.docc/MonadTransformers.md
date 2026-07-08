# Monad Transformers

How ReactiveConcurrency stacks its effects (``DeferredTask``, ``DeferredStream``, ``Publisher``)
with the FP library's data structures — and why the matrix is intentionally one-directional.

## Overview

A *monad transformer* combines two type constructors so their capabilities compose. RC ships
transformers between its three **effects** and the FP `DataStructure` types (`Either`, `Result`,
`Optional`, `Array`, `Validation`, `Reader`, `Writer`, `Stateful`), in the `ReactiveConcurrencyTransformers`
product, with symbolic operators in `ReactiveConcurrencyOperators`.

A stack `XTY` is the concrete type `X<Y<A>>` — `X` is the *outer* type, `Y` the *inner*. Each stack
ships one file per capability (`+Functor`, `+Applicative`, `+Monad`), and each capability has a
free-function form (`mapT…`, `applyT…`/`liftA2…`, `flatMapT…`/`bindT…`/`kleisliT`) plus operators
(`<£^>`/`<&^>`, `<*>`/`*>`/`<*`, `>>-`/`-<<`/`>=>`/`<=<`).

## The matrix

Every combination below is a lawful transformer with its full capability set, verified by the
`TransformerLawTests` (left/right identity, associativity, `>=>`/`<=<` consistency).

| Direction | Stacks | Functor | Applicative | Monad |
|---|---|---|---|---|
| **Effect-outer, structure-inner** | `{DeferredTask, DeferredStream, Publisher}T{Array, Either, Optional, Result}` | ✓ | ✓ | ✓ |
| Effect-outer, Validation-inner | `…TValidation` | ✓ | ✓ (accumulates) | — |
| Effect-outer, Writer-inner | `…TWriter` | ✓ | ✓ | ✓ (combines logs) |
| **Reader-outer, effect-inner** | `ReaderT{effect}` | ✓ | ✓ | ✓ |
| Stateful-outer, effect-inner | `StatefulT{effect}` | ✓ | ✓ | — |

`Optional`/`Result` effect-outer stacks additionally have a lawful, left-biased `Alternative`
(`<|>`), with a concurrent `race…` counterpart.

## Why the matrix is one-directional

For each structure family, RC ships **only the direction that yields a lawful, runnable transformer**.
The reverse directions are deliberately absent:

- **`Validation` is Applicative-only.** Its `flatMap` would short-circuit on the first failure while
  its `apply` accumulates errors — violating the Applicative/Monad consistency law. Convert to
  `Result`/`Either` for short-circuit sequencing. (This matches FP 2.0, which removed the same
  instances.)

- **Container-outer / effect-inner** (e.g. `Either<L, DeferredTask<A>>`) is **not provided**: you
  cannot synchronously extract an *async* result to drive the outer structure's `flatMap`, so no
  lawful `Monad` exists — only Functor/Applicative, which the effect-outer direction already covers.

- **Writer-outer / effect-inner** (`Writer<W, DeferredTask<A>>`) is **not provided**: with the log
  *outside* the effect, `bind` cannot observe the continuation's log without running the effect. RC
  instead uses **effect-outer** `…TWriter` (`DeferredTask<Writer<W, A>>`), where `bind` combines
  `w1 <> w2` inside the effect — the lawful shape.

- **`Stateful` has no `Monad`** in any stack: an `inout` state parameter cannot be captured across an
  async `@Sendable` boundary. Use `Reader<Env, effect>` (`ReaderT…`) for monadic chaining with a
  read-only environment.

- **Effect-outer / Reader-inner** (`DeferredTask<Reader<Env, A>>`) is **not expressible**: `Reader`
  is not `Sendable`, so it can't be an effect element. Use `ReaderT…` instead.

## Traversals

Turn a container of effects into an effect of a container: `sequence…` and `traverse…`. `DeferredTask`
runs them sequentially; `DeferredStream` and `Publisher` are zippy (positional, truncating to the
shortest input, consistent with their ZipList-style applicative).
