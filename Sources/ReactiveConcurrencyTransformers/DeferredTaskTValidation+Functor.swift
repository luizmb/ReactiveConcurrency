// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredTaskTValidation: outer = DeferredTask, inner = Validation
// Type: DeferredTask<Validation<E, A>>

/// Functor map over the DeferredTask-over-Validation stack: transforms the innermost value, leaving the DeferredTask and Validation layers intact.
public func mapTDeferredTaskValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Validation<E, A>>
) -> DeferredTask<Validation<E, B>> {
    task.map { v in v.mapSuccess(fn) }
}

/// Functor map (point-free) for the DeferredTask-over-Validation stack: transforms the innermost value, leaving the DeferredTask and Validation
/// layers intact.
public func fmapTDeferredTaskValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredTask<Validation<E, A>>) -> DeferredTask<Validation<E, B>> {
    { @Sendable task in mapTDeferredTaskValidation(fn, task) }
}
