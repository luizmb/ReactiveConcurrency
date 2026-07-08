// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTArray: outer = DeferredStream, inner = Array
// Type: DeferredStream<[A]>

/// Applicative liftA2 for the DeferredStream-over-Array stack: runs both effects and combines their results with fn.
public func liftA2TDeferredStreamArray<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<[A]>, DeferredStream<[B]>) -> DeferredStream<[C]> {
    { @Sendable sa, sb in
        liftA2DeferredStream { arrA, arrB in
            arrA.flatMap { a in arrB.map { b in fn(a, b) } }
        }(sa, sb)
    }
}

/// Applicative apply for the DeferredStream-over-Array stack.
public func applyTDeferredStreamArray<A: Sendable, B: Sendable>(
    _ fns: DeferredStream<[@Sendable (A) -> B]>,
    _ values: DeferredStream<[A]>
) -> DeferredStream<[B]> {
    liftA2DeferredStream { arrF, arrA in
        arrF.flatMap { f in arrA.map { a in f(a) } }
    }(fns, values)
}
