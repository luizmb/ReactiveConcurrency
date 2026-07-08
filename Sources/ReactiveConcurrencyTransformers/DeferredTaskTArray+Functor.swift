// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTArray: outer = DeferredTask, inner = Array
// Type: DeferredTask<[A]>  — Haskell: ListT DeferredTask

/// Functor map over the DeferredTask-over-Array stack: transforms the innermost value, leaving the DeferredTask and Array layers intact.
public func mapTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<[A]>
) -> DeferredTask<[B]> {
    task.map { arr in arr.map(fn) }
}

/// Functor map (point-free) for the DeferredTask-over-Array stack: transforms the innermost value, leaving the DeferredTask and Array layers intact.
public func fmapTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredTask<[A]>) -> DeferredTask<[B]> {
    { @Sendable task in mapTDeferredTaskArray(fn, task) }
}
