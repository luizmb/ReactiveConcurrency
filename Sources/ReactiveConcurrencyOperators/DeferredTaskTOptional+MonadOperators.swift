// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<a?> -> (a -> DeferredTask<b?>) -> DeferredTask<b?>

/// Monadic bind — sequences a dependent effect (container on the left) for the DeferredTask-over-Optional stack. Operator form of `flatMap`.
public func >>- <A: Sendable, B: Sendable>(
    _ task: DeferredTask<A?>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<B?>
) -> DeferredTask<B?> {
    flatMapTDeferredTaskOptional(task, fn)
}

// (-<<) :: (a -> DeferredTask<b?>) -> DeferredTask<a?> -> DeferredTask<b?>

/// Monadic bind — sequences a dependent effect (function on the left) for the DeferredTask-over-Optional stack. Operator form of `flatMap`.
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<B?>,
    _ task: DeferredTask<A?>
) -> DeferredTask<B?> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask<b?>) -> (b -> DeferredTask<c?>) -> a -> DeferredTask<c?>

/// Left-to-right Kleisli composition of two effectful functions for the DeferredTask-over-Optional stack.
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<B?>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<C?>
) -> @Sendable (A) -> DeferredTask<C?> {
    kleisliTDeferredTaskOptional(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<c?>) -> (a -> DeferredTask<b?>) -> a -> DeferredTask<c?>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the DeferredTask-over-Optional stack.
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<C?>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<B?>
) -> @Sendable (A) -> DeferredTask<C?> {
    fn1 >=> fn2
}
