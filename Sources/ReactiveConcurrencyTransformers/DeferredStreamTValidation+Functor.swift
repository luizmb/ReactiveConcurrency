// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredStreamTValidation: outer = DeferredStream, inner = Validation
// Type: DeferredStream<Validation<E, A>>
/// Functor map over the DeferredStream-over-Validation stack: transforms the innermost value, leaving the DeferredStream and Validation layers
/// intact.
public func mapTDeferredStreamValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Validation<E, A>>
) -> DeferredStream<Validation<E, B>> {
    stream.map { v in v.mapSuccess(fn) }
}

/// Functor map (point-free) for the DeferredStream-over-Validation stack: transforms the innermost value, leaving the DeferredStream and Validation
/// layers intact.
public func fmapTDeferredStreamValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<Validation<E, A>>) -> DeferredStream<Validation<E, B>> {
    { @Sendable stream in mapTDeferredStreamValidation(fn, stream) }
}
