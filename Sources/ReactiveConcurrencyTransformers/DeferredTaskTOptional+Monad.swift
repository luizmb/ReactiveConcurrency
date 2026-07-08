// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: outer = DeferredTask, inner = Optional
// Type: DeferredTask<A?>

// flatMapT :: DeferredTask<A?> -> (A -> DeferredTask<B?>) -> DeferredTask<B?>
// nil short-circuits; Some(a) proceeds through fn
/// Monadic bind for the DeferredTask-over-Optional stack: nil short-circuits; a present value threads through fn.
public func flatMapTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ task: DeferredTask<A?>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<B?>
) -> DeferredTask<B?> {
    task.flatMap { optA in
        guard let a = optA else { return .pure(nil) }
        return fn(a)
    }
}

/// Monadic bind (point-free) for the DeferredTask-over-Optional stack: nil short-circuits; a present value threads through fn.
public func bindTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<B?>
) -> @Sendable (DeferredTask<A?>) -> DeferredTask<B?> {
    { @Sendable task in flatMapTDeferredTaskOptional(task, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
/// Left-to-right Kleisli composition for the DeferredTask-over-Optional stack.
public func kleisliTDeferredTaskOptional<A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<B?>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<C?>
) -> @Sendable (A) -> DeferredTask<C?> {
    { @Sendable a in flatMapTDeferredTaskOptional(fn1(a), fn2) }
}
