// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: outer = DeferredTask, inner = Optional
// Type: DeferredTask<A?>

/// Applicative liftA2 for the DeferredTask-over-Optional stack: runs both effects and combines their results with fn.
public func liftA2TDeferredTaskOptional<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredTask<A?>, DeferredTask<B?>) -> DeferredTask<C?> {
    { @Sendable ta, tb in
        liftA2DeferredTask { optA, optB -> C? in
            guard let a = optA, let b = optB else { return nil }
            return fn(a, b)
        }(ta, tb)
    }
}

/// Applicative apply for the DeferredTask-over-Optional stack.
public func applyTDeferredTaskOptional<A: Sendable, B: Sendable>(
    _ fns: DeferredTask<(@Sendable (A) -> B)?>,
    _ values: DeferredTask<A?>
) -> DeferredTask<B?> {
    liftA2DeferredTask { optF, optA -> B? in
        guard let f = optF, let a = optA else { return nil }
        return f(a)
    }(fns, values)
}
