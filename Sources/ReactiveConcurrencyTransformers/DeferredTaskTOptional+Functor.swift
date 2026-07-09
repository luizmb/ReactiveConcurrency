// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: outer = DeferredTask, inner = Optional
// Type: DeferredTask<A?>  — Haskell: MaybeT DeferredTask

/// Functor map over the DeferredTask-over-Optional stack: transforms the innermost value, leaving the DeferredTask and Optional layers intact.
public func mapTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<A?>
) -> DeferredTask<B?> {
    task.map { optA in optA.map(fn) }
}

/// Functor map (point-free) for the DeferredTask-over-Optional stack: transforms the innermost value, leaving the DeferredTask and Optional layers
/// intact.
public func fmapTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredTask<A?>) -> DeferredTask<B?> {
    { @Sendable task in mapTDeferredTaskOptional(fn, task) }
}
