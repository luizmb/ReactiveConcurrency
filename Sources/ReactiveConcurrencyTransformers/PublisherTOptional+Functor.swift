// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: outer = Publisher, inner = Optional
// Type: Publisher<A?, F>  — MaybeT over a typed-failure Publisher.

/// Functor map over the Publisher-over-Optional stack: transforms the innermost value, leaving the Publisher and Optional layers intact.
public func mapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<A?, F>
) -> Publisher<B?, F> {
    publisher.map { optA in optA.map(fn) }
}

/// Functor map (point-free) for the Publisher-over-Optional stack: transforms the innermost value, leaving the Publisher and Optional layers intact.
public func fmapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<A?, F>) -> Publisher<B?, F> {
    { @Sendable publisher in mapTPublisherOptional(fn, publisher) }
}
