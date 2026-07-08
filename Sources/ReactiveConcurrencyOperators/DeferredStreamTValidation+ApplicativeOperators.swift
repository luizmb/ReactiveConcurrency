// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<Validation<e,a->b>> -> DeferredStream<Validation<e,a>> -> DeferredStream<Validation<e,b>>

/// Applicative apply for the DeferredStream-over-Validation stack.
public func <*> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredStream<Validation<E, @Sendable (A) -> B>>,
    _ values: DeferredStream<Validation<E, A>>
) -> DeferredStream<Validation<E, B>> {
    applyTDeferredStreamValidation(fns, values)
}
