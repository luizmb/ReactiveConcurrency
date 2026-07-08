// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredTask<Validation<e,a->b>> -> DeferredTask<Validation<e,a>> -> DeferredTask<Validation<e,b>>

/// Applicative apply for the DeferredTask-over-Validation stack.
public func <*> <E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredTask<Validation<E, @Sendable (A) -> B>>,
    _ values: DeferredTask<Validation<E, A>>
) -> DeferredTask<Validation<E, B>> {
    applyTDeferredTaskValidation(fns, values)
}
