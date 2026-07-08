// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<Writer<w, a>> -> (a -> DeferredTask<Writer<w, b>>) -> DeferredTask<Writer<w, b>>

/// Monadic bind — sequences a dependent effect (container on the left) for the DeferredTask-over-Writer stack. Operator form of `flatMap`.
public func >>- <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Writer<W, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, B>> {
    task.flatMapT(fn)
}

// (-<<) :: (a -> DeferredTask<Writer<w, b>>) -> DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>>

/// Monadic bind — sequences a dependent effect (function on the left) for the DeferredTask-over-Writer stack. Operator form of `flatMap`.
public func -<< <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>,
    _ task: DeferredTask<Writer<W, A>>
) -> DeferredTask<Writer<W, B>> {
    task.flatMapT(fn)
}

// (>=>) :: (a -> DeferredTask<Writer<w, b>>) -> (b -> DeferredTask<Writer<w, c>>) -> a -> DeferredTask<Writer<w, c>>

/// Left-to-right Kleisli composition of two effectful functions for the DeferredTask-over-Writer stack.
public func >=> <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Writer<W, C>>
) -> @Sendable (A) -> DeferredTask<Writer<W, C>> {
    kleisliTDeferredTaskWriter(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<Writer<w, c>>) -> (a -> DeferredTask<Writer<w, b>>) -> a -> DeferredTask<Writer<w, c>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the DeferredTask-over-Writer stack.
public func <=< <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Writer<W, C>>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>
) -> @Sendable (A) -> DeferredTask<Writer<W, C>> {
    fn1 >=> fn2
}
