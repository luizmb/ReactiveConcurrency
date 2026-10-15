// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators

// FP 3 dropped `<£^>` / `<&^>` (its transformer stacks are structs that reuse `<£>` / `<&>`).
// ReactiveConcurrency still lifts maps through its nested stream/task shapes with them, so the
// declarations live here now, in the same precedence groups FP 2 used.

/// Transformer fmap: `(<£^>) :: (a -> b) -> f (g a) -> f (g b)`. Operator form of `mapT`; only
/// transformer overloads exist, so the stack is always picked without ambiguity.
infix operator <£^>: FunctorOps

/// Flipped transformer fmap: `(<&^>) :: f (g a) -> (a -> b) -> f (g b)`.
infix operator <&^>: MonadBindLeft
