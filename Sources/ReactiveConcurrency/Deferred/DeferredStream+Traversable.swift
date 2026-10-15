// SPDX-License-Identifier: Apache-2.0

// Traversals for DeferredStream, built on its applicative (`ap`, the cartesian product derived from
// the concat bind): `sequence` emits every combination of one element from each stream, in order
// (the first stream varies slowest). For positional pairing of streams use `zip`.

/// Turns an array of streams into one stream of arrays: every combination of one element per
/// stream, in order (the first stream varies slowest). An empty input emits `[]` once.
public func sequenceDeferredStream<A: Sendable>(_ streams: [DeferredStream<A>]) -> DeferredStream<[A]> {
    streams.reversed().reduce(DeferredStream<[A]>.pure([])) { acc, stream in
        liftA2DeferredStream { (a: A, rest: [A]) in [a] + rest }(stream, acc)
    }
}

/// Maps each element to a stream and sequences the results (see `sequenceDeferredStream`).
public func traverseDeferredStream<A: Sendable, B: Sendable>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> DeferredStream<B>
) -> DeferredStream<[B]> {
    sequenceDeferredStream(xs.map(transform))
}
