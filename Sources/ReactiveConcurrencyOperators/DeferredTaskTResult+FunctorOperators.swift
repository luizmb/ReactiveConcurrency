// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredTask<Result<a,e>> -> DeferredTask<Result<b,e>>

/// Functor map lifted through the transformer (function on the left) for the DeferredTask-over-Result stack. Operator form of `mapT`.
public func <£^> <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Result<A, E>>
) -> DeferredTask<Result<B, E>> {
    mapTDeferredTaskResult(fn, task)
}

// (<&^>) :: DeferredTask<Result<a,e>> -> (a -> b) -> DeferredTask<Result<b,e>>

/// Functor map lifted through the transformer (container on the left) for the DeferredTask-over-Result stack. Operator form of `mapT`.
public func <&^> <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ task: DeferredTask<Result<A, E>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredTask<Result<B, E>> {
    mapTDeferredTaskResult(fn, task)
}
