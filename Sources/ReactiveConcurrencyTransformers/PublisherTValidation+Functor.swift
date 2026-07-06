// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>

public func mapTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    publisher.map { v in v.mapSuccess(fn) }
}

public func fmapTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<Validation<E, A>, F>) -> Publisher<Validation<E, B>, F> {
    { @Sendable publisher in mapTPublisherValidation(fn, publisher) }
}
