// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Validation<e,a>, f> -> (a -> Publisher<Validation<e,b>, f>) -> Publisher<Validation<e,b>, f>
public func >>- <E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Validation<E, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Validation<E, B>, F>
) -> Publisher<Validation<E, B>, F> {
    flatMapTPublisherValidation(publisher, fn)
}

// (-<<) :: (a -> Publisher<Validation<e,b>, f>) -> Publisher<Validation<e,a>, f> -> Publisher<Validation<e,b>, f>
public func -<< <E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Validation<E, B>, F>,
    _ publisher: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    publisher >>- fn
}
