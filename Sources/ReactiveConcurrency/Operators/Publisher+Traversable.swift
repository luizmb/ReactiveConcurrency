// SPDX-License-Identifier: Apache-2.0

// Traversals for Publisher, built on its applicative (`ap`, the cartesian product derived from the
// ordered concat bind): `sequence` emits every combination of one value from each publisher, in
// order (the first publisher varies slowest). A failure from any publisher propagates. For
// positional pairing use `zip`.

/// Turns an array of publishers into a publisher of arrays: every combination of one value per
/// publisher, in order (the first publisher varies slowest). An empty input emits `[]` once. A
/// failure from any publisher propagates.
public func sequencePublisher<A: Sendable, Failure: Error>(
    _ publishers: [Publisher<A, Failure>]
) -> Publisher<[A], Failure> {
    publishers.reversed().reduce(Publisher<[A], Failure>.just([])) { acc, publisher in
        liftA2Publisher { (a: A, rest: [A]) in [a] + rest }(publisher, acc)
    }
}

/// Maps each element of `xs` to a publisher via `transform`, then sequences them (see
/// `sequencePublisher`).
public func traversePublisher<A: Sendable, B: Sendable, Failure: Error>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> Publisher<B, Failure>
) -> Publisher<[B], Failure> {
    sequencePublisher(xs.map(transform))
}
