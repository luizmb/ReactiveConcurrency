// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredTaskTWriter: the WriterT monad transformer over DeferredTask.
// Representation: DeferredTask<Writer<W, A>>
//
// flatMapT runs the outer effect to obtain (a, w1), runs the continuation fn(a) to obtain
// (b, w2), and combines the logs w1 <> w2 INSIDE the effect. This is the lawful WriterT bind
// (left/right identity + associativity hold); the previous shape discarded fn(a)'s log.

public extension DeferredTask {
    /// Monadic bind for the DeferredTask-over-Writer stack: threads the value through fn and combines the accumulated logs via the Monoid.
    func flatMapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> DeferredTask<Writer<W, B>>
    ) -> DeferredTask<Writer<W, B>>
    where Success == Writer<W, Inner> {
        flatMap { w1 in
            fn(w1.value).map { w2 in Writer<W, B>(w2.value, W.combine(w1.log, w2.log)) }
        }
    }

    /// Monadic bind (point-free) for the DeferredTask-over-Writer stack: threads the value through fn and combines the accumulated logs via the
    /// Monoid.
    static func bindT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> DeferredTask<Writer<W, B>>
    ) -> @Sendable (DeferredTask<Writer<W, Inner>>) -> DeferredTask<Writer<W, B>> {
        { $0.flatMapT(fn) }
    }
}

/// Left-to-right Kleisli composition for the DeferredTask-over-Writer stack.
public func kleisliTDeferredTaskWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Writer<W, C>>
) -> @Sendable (A) -> DeferredTask<Writer<W, C>> {
    { @Sendable a in fn1(a).flatMapT(fn2) }
}
