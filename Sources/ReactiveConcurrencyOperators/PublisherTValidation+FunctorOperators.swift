// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Publisher<Validation<e,a>, f> -> Publisher<Validation<e,b>, f>

/// Functor map lifted through the transformer (function on the left) for the Publisher-over-Validation stack. Operator form of `mapT`.
public func <£^> <E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    mapTPublisherValidation(fn, publisher)
}

// (<&^>) :: Publisher<Validation<e,a>, f> -> (a -> b) -> Publisher<Validation<e,b>, f>

/// Functor map lifted through the transformer (container on the left) for the Publisher-over-Validation stack. Operator form of `mapT`.
public func <&^> <E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Validation<E, A>, F>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<Validation<E, B>, F> {
    mapTPublisherValidation(fn, publisher)
}
