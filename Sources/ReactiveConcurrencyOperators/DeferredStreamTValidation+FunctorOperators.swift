// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredStream<Validation<e,a>> -> DeferredStream<Validation<e,b>>

/// Functor map lifted through the transformer (function on the left) for the DeferredStream-over-Validation stack. Operator form of `mapT`.
public func <£^> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Validation<E, A>>
) -> DeferredStream<Validation<E, B>> {
    mapTDeferredStreamValidation(fn, stream)
}

// (<&^>) :: DeferredStream<Validation<e,a>> -> (a -> b) -> DeferredStream<Validation<e,b>>

/// Functor map lifted through the transformer (container on the left) for the DeferredStream-over-Validation stack. Operator form of `mapT`.
public func <&^> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Validation<E, A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<Validation<E, B>> {
    mapTDeferredStreamValidation(fn, stream)
}
