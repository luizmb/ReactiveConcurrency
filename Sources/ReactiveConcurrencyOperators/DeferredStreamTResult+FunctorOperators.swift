// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredStream<Result<a,e>> -> DeferredStream<Result<b,e>>

/// Functor map lifted through the transformer (function on the left) for the DeferredStream-over-Result stack. Operator form of `mapT`.
public func <£^> <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Result<A, E>>
) -> DeferredStream<Result<B, E>> {
    mapTDeferredStreamResult(fn, stream)
}

// (<&^>) :: DeferredStream<Result<a,e>> -> (a -> b) -> DeferredStream<Result<b,e>>

/// Functor map lifted through the transformer (container on the left) for the DeferredStream-over-Result stack. Operator form of `mapT`.
public func <&^> <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ stream: DeferredStream<Result<A, E>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<Result<B, E>> {
    mapTDeferredStreamResult(fn, stream)
}
