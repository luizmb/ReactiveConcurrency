// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTArray: outer = DeferredTask, inner = Array
// Type: DeferredTask<[A]>

// flatMapT :: DeferredTask<[A]> -> (A -> DeferredTask<[B]>) -> DeferredTask<[B]>
// Applies fn to each element, runs tasks sequentially, concatenates results.
/// Monadic bind for the DeferredTask-over-Array stack: binds fn across every element and concatenates the results (sequential).
public func flatMapTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ task: DeferredTask<[A]>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<[B]>
) -> DeferredTask<[B]> {
    task.flatMap { arr in
        DeferredTask<[B]> {
            var result: [B] = []
            for a in arr {
                let bs = await fn(a).run()
                result.append(contentsOf: bs)
            }
            return result
        }
    }
}

/// Monadic bind (point-free) for the DeferredTask-over-Array stack: binds fn across every element and concatenates the results (sequential).
public func bindTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<[B]>
) -> @Sendable (DeferredTask<[A]>) -> DeferredTask<[B]> {
    { @Sendable task in flatMapTDeferredTaskArray(task, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
/// Left-to-right Kleisli composition for the DeferredTask-over-Array stack.
public func kleisliTDeferredTaskArray<A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<[B]>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<[C]>
) -> @Sendable (A) -> DeferredTask<[C]> {
    { @Sendable a in flatMapTDeferredTaskArray(fn1(a), fn2) }
}
