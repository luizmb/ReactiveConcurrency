// SPDX-License-Identifier: Apache-2.0

// Traversals for Publisher. Publisher's product is zippy (see Publisher.zip / Publisher+FP), so
// `sequence` zips the publishers positionally — the i-th emission is the array of every
// publisher's i-th value, and it completes with the shortest input. A failure from any publisher
// propagates (via zip). Base case is `last.map { [$0] }` for the same ZipList reason as
// DeferredStream: a single-shot `pure` zipped against a multi-value publisher would truncate.
/// Turns an array of publishers into a publisher of arrays by zipping them positionally: the
/// i-th emission is every publisher's i-th value, completing with the shortest input (zippy, not
/// cartesian). A failure from any publisher propagates.
public func sequencePublisher<A: Sendable, Failure: Error>(
    _ publishers: [Publisher<A, Failure>]
) -> Publisher<[A], Failure> {
    guard let last = publishers.last else { return Publisher<[A], Failure>.just([]) }
    let initial = last.map { [$0] }
    return publishers.dropLast().reversed().reduce(initial) { acc, publisher in
        publisher.zip(acc).map { [$0.0] + $0.1 }
    }
}

/// Maps each element of `xs` to a publisher via `transform`, then zips them positionally into a
/// publisher of arrays (see `sequencePublisher`).
public func traversePublisher<A: Sendable, B: Sendable, Failure: Error>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> Publisher<B, Failure>
) -> Publisher<[B], Failure> {
    sequencePublisher(xs.map(transform))
}
