// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTArray: outer = Publisher, inner = Array
// Type: Publisher<[A], F>  — ListT over a typed-failure Publisher.

public func mapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<[A], F>
) -> Publisher<[B], F> {
    publisher.map { arr in arr.map(fn) }
}

public func fmapTPublisherArray<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<[A], F>) -> Publisher<[B], F> {
    { @Sendable publisher in mapTPublisherArray(fn, publisher) }
}
