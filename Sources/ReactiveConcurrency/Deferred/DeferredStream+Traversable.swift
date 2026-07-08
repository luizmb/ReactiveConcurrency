// SPDX-License-Identifier: Apache-2.0

// Traversals for DeferredStream. DeferredStream's applicative is zippy (ZipList-style), so
// `sequence` zips the streams positionally: the i-th element of the result is the array of every
// stream's i-th element, and the result length is the SHORTEST input stream's length.
//
// Note the base case is `last.map { [$0] }`, not `pure([])`: a single-shot `pure` zipped against a
// multi-element stream would truncate everything to length 1 (the classic ZipList `pure` defect).
///
/// Zippy (ZipList-style): the i-th emitted array holds every stream's i-th element, and the result
/// length is that of the shortest input stream.

/// Turns an array of streams into one stream of arrays, zipping positionally.
public func sequenceDeferredStream<A: Sendable>(_ streams: [DeferredStream<A>]) -> DeferredStream<[A]> {
    guard let last = streams.last else { return DeferredStream<[A]>.pure([]) }
    let initial = last.map { [$0] }
    return streams.dropLast().reversed().reduce(initial) { acc, stream in
        liftA2DeferredStream { (a: A, rest: [A]) in [a] + rest }(stream, acc)
    }
}

/// Maps each element to a stream and zips the results positionally into one stream of arrays.
public func traverseDeferredStream<A: Sendable, B: Sendable>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> DeferredStream<B>
) -> DeferredStream<[B]> {
    sequenceDeferredStream(xs.map(transform))
}
