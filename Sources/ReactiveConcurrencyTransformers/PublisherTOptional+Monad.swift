// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: outer = Publisher, inner = Optional
// Type: Publisher<A?, F>

// flatMapT: nil elements pass through as nil; .some(a) is replaced by the elements of fn(a).
// Sequential (maxPublishers: 1) preserves emission order.
/// Monadic bind for the Publisher-over-Optional stack: nil short-circuits; a present value threads through fn.
func flatMapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<A?, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<B?, F>
) -> Publisher<B?, F> {
    publisher.flatMap(maxPublishers: 1) { optA in
        if let a = optA { fn(a) } else { Publisher<B?, F>.just(nil) }
    }
}
