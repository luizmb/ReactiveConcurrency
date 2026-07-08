// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTOptional: outer = DeferredStream, inner = Optional
// Type: DeferredStream<A?>

// liftA2T: combines two DeferredStream<A?> applicatively — nil propagates
/// Applicative liftA2 for the DeferredStream-over-Optional stack: runs both effects and combines their results with fn.
public func liftA2TDeferredStreamOptional<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<A?>, DeferredStream<B?>) -> DeferredStream<C?> {
    { @Sendable sa, sb in
        liftA2DeferredStream { optA, optB -> C? in
            guard let a = optA, let b = optB else { return nil }
            return fn(a, b)
        }(sa, sb)
    }
}

/// Applicative apply for the DeferredStream-over-Optional stack.
public func applyTDeferredStreamOptional<A: Sendable, B: Sendable>(
    _ fns: DeferredStream<(@Sendable (A) -> B)?>,
    _ values: DeferredStream<A?>
) -> DeferredStream<B?> {
    liftA2DeferredStream { optF, optA -> B? in
        guard let f = optF, let a = optA else { return nil }
        return f(a)
    }(fns, values)
}
