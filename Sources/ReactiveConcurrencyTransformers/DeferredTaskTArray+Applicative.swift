// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTArray: outer = DeferredTask, inner = Array
// Type: DeferredTask<[A]>

/// Applicative liftA2 for the DeferredTask-over-Array stack: runs both effects and combines their results with fn.
public func liftA2TDeferredTaskArray<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredTask<[A]>, DeferredTask<[B]>) -> DeferredTask<[C]> {
    { @Sendable ta, tb in
        liftA2DeferredTask { arrA, arrB in
            arrA.flatMap { a in arrB.map { b in fn(a, b) } }
        }(ta, tb)
    }
}

/// Applicative apply for the DeferredTask-over-Array stack.
public func applyTDeferredTaskArray<A: Sendable, B: Sendable>(
    _ fns: DeferredTask<[@Sendable (A) -> B]>,
    _ values: DeferredTask<[A]>
) -> DeferredTask<[B]> {
    liftA2DeferredTask { arrF, arrA in
        arrF.flatMap { f in arrA.map { a in f(a) } }
    }(fns, values)
}
