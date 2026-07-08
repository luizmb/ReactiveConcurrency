// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredTask<Validation<e,a>> -> DeferredTask<Validation<e,b>>

/// Functor map lifted through the transformer (function on the left) for the DeferredTask-over-Validation stack. Operator form of `mapT`.
public func <£^> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Validation<E, A>>
) -> DeferredTask<Validation<E, B>> {
    mapTDeferredTaskValidation(fn, task)
}

// (<&^>) :: DeferredTask<Validation<e,a>> -> (a -> b) -> DeferredTask<Validation<e,b>>

/// Functor map lifted through the transformer (container on the left) for the DeferredTask-over-Validation stack. Operator form of `mapT`.
public func <&^> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Validation<E, A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredTask<Validation<E, B>> {
    mapTDeferredTaskValidation(fn, task)
}
