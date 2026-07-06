// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>. Inner Array applicative is the cartesian product.

public func liftA2TPublisherArray<A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<[A], F>, Publisher<[B], F>) -> Publisher<[C], F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair in
            pair.0.flatMap { a in pair.1.map { b in fn(a, b) } }
        }
    }
}

public func applyTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<[@Sendable (A) -> B], F>,
    _ values: Publisher<[A], F>
) -> Publisher<[B], F> {
    fns.zip(values).map { pair in
        pair.0.flatMap { f in pair.1.map { a in f(a) } }
    }
}
