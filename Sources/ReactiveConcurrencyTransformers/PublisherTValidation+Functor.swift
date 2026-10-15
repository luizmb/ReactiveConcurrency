// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>

/// Functor map over the Publisher-over-Validation stack: transforms the innermost value, leaving the Publisher and Validation layers intact.
func mapTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    publisher.map { v in v.mapSuccess(fn) }
}
