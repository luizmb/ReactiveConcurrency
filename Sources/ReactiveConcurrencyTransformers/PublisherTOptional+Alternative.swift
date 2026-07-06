// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: Alternative
// Type: Publisher<A?, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when present, otherwise the right (`optA ?? optB`). This mirrors the zip-based applicative
// used by the other PublisherT* combinators (DeferredStream has no Alternative precedent, so
// the stream semantics are defined here rather than the single-value "first non-nil" of
// DeferredTaskTOptional).
public func altPublisherOptional<A: Sendable, F: Error>(
    _ lhs: Publisher<A?, F>,
    _ rhs: @autoclosure () -> Publisher<A?, F>
) -> Publisher<A?, F> {
    lhs.zip(rhs()).map { pair in pair.0 ?? pair.1 }
}
