// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: outer = DeferredTask, inner = Optional
// Type: DeferredTask<A?>

// flatMapT :: DeferredTask<A?> -> (A -> DeferredTask<B?>) -> DeferredTask<B?>
// nil short-circuits; Some(a) proceeds through fn
/// Monadic bind for the DeferredTask-over-Optional stack: nil short-circuits; a present value threads through fn.
func flatMapTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ task: DeferredTask<A?>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<B?>
) -> DeferredTask<B?> {
    task.flatMap { optA in
        guard let a = optA else { return .pure(nil) }
        return fn(a)
    }
}
