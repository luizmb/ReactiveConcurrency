// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: outer = Publisher, inner = Optional
// Type: Publisher<A?, F>

// flatMapT: nil elements pass through as nil; .some(a) is replaced by the elements of fn(a).
// Sequential (maxPublishers: 1) preserves emission order.
public func flatMapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<A?, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<B?, F>
) -> Publisher<B?, F> {
    publisher.flatMap(maxPublishers: 1) { optA in
        if let a = optA { fn(a) } else { Publisher<B?, F>.just(nil) }
    }
}

public func bindTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<B?, F>
) -> @Sendable (Publisher<A?, F>) -> Publisher<B?, F> {
    { @Sendable publisher in flatMapTPublisherOptional(publisher, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
public func kleisliTPublisherOptional<A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<B?, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<C?, F>
) -> @Sendable (A) -> Publisher<C?, F> {
    { @Sendable a in flatMapTPublisherOptional(fn1(a), fn2) }
}
