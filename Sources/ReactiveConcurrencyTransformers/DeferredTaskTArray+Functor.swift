// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTArray: outer = DeferredTask, inner = Array
// Type: DeferredTask<[A]>  — Haskell: ListT DeferredTask

/// Functor map over the DeferredTask-over-Array stack: transforms the innermost value, leaving the DeferredTask and Array layers intact.
func mapTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<[A]>
) -> DeferredTask<[B]> {
    task.map { arr in arr.map(fn) }
}
