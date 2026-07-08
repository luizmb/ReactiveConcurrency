// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>

// flatMapT: for each emitted [A], apply fn to every element and concatenate all resulting [B]
// arrays into one [B] (matching the ListT concat). Sequential via flatMap(maxPublishers: 1)
// preserves emission order; each element's fn output is folded with reduce + zip.
/// Monadic bind for the Publisher-over-Array stack: binds fn across every element and concatenates the results (sequential).
public func flatMapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<[A], F>,
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>
) -> Publisher<[B], F> {
    publisher.flatMap(maxPublishers: 1) { (arrA: [A]) -> Publisher<[B], F> in
        arrA.reduce(Publisher<[B], F>.just([])) { acc, a in
            let flattened = fn(a).reduce([B]()) { $0 + $1 }
            return acc.zip(flattened).map { $0.0 + $0.1 }
        }
    }
}

/// Monadic bind (point-free) for the Publisher-over-Array stack: binds fn across every element and concatenates the results (sequential).
public func bindTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>
) -> @Sendable (Publisher<[A], F>) -> Publisher<[B], F> {
    { @Sendable publisher in flatMapTPublisherArray(publisher, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
/// Left-to-right Kleisli composition for the Publisher-over-Array stack.
public func kleisliTPublisherArray<A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<[B], F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<[C], F>
) -> @Sendable (A) -> Publisher<[C], F> {
    { @Sendable a in flatMapTPublisherArray(fn1(a), fn2) }
}
