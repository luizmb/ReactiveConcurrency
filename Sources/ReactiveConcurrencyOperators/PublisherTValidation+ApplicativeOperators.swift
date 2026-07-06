// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Publisher<Validation<e,a->b>, f> -> Publisher<Validation<e,a>, f> -> Publisher<Validation<e,b>, f>
// Accumulates errors via the Semigroup (Validation applicative).
public func <*> <E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Validation<E, @Sendable (A) -> B>, F>,
    _ values: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    applyTPublisherValidation(fns, values)
}
