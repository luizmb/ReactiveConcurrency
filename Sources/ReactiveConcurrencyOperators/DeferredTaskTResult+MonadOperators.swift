// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<Result<a,e>> -> (a -> DeferredTask<Result<b,e>>) -> DeferredTask<Result<b,e>>

/// Monadic bind — sequences a dependent effect (container on the left) for the DeferredTask-over-Result stack. Operator form of `flatMap`.
public func >>- <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ task: DeferredTask<Result<A, E>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>
) -> DeferredTask<Result<B, E>> {
    flatMapTDeferredTaskResult(task, fn)
}

// (-<<) :: (a -> DeferredTask<Result<b,e>>) -> DeferredTask<Result<a,e>> -> DeferredTask<Result<b,e>>

/// Monadic bind — sequences a dependent effect (function on the left) for the DeferredTask-over-Result stack. Operator form of `flatMap`.
public func -<< <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>,
    _ task: DeferredTask<Result<A, E>>
) -> DeferredTask<Result<B, E>> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask<Result<b,e>>) -> (b -> DeferredTask<Result<c,e>>) -> a -> DeferredTask<Result<c,e>>

/// Left-to-right Kleisli composition of two effectful functions for the DeferredTask-over-Result stack.
public func >=> <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Result<C, E>>
) -> @Sendable (A) -> DeferredTask<Result<C, E>> {
    kleisliTDeferredTaskResult(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<Result<c,e>>) -> (a -> DeferredTask<Result<b,e>>) -> a -> DeferredTask<Result<c,e>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the DeferredTask-over-Result stack.
public func <=< <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Result<C, E>>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>
) -> @Sendable (A) -> DeferredTask<Result<C, E>> {
    fn1 >=> fn2
}
