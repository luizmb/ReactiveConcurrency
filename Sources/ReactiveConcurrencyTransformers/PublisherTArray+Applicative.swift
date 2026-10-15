// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>. Inner Array applicative is the cartesian product.

/// Applicative liftA2 for the Publisher-over-Array stack: runs both effects and combines their results with fn.
func liftA2TPublisherArray<A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<[A], F>, Publisher<[B], F>) -> Publisher<[C], F> {
    { @Sendable pa, pb in
        liftA2Publisher { (arrA: [A], arrB: [B]) in
            arrA.flatMap { a in arrB.map { b in fn(a, b) } }
        }(pa, pb)
    }
}

/// Applicative apply for the Publisher-over-Array stack: cartesian over the publisher, then over each pair of arrays.
func applyTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<[@Sendable (A) -> B], F>,
    _ values: Publisher<[A], F>
) -> Publisher<[B], F> {
    liftA2Publisher { (arrF: [@Sendable (A) -> B], arrA: [A]) in
        arrF.flatMap { f in arrA.map { a in f(a) } }
    }(fns, values)
}
