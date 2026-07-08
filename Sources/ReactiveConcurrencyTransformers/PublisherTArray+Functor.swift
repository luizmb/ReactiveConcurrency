// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>  — ListT over a typed-failure Publisher.

/// Functor map over the Publisher-over-Array stack: transforms the innermost value, leaving the Publisher and Array layers intact.
public func mapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<[A], F>
) -> Publisher<[B], F> {
    publisher.map { arr in arr.map(fn) }
}

/// Functor map (point-free) for the Publisher-over-Array stack: transforms the innermost value, leaving the Publisher and Array layers intact.
public func fmapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<[A], F>) -> Publisher<[B], F> {
    { @Sendable publisher in mapTPublisherArray(fn, publisher) }
}
