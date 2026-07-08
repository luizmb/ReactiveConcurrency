// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: zippy element-wise choice (NOT a lawful Alternative)
// Type: Publisher<A?, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when present, otherwise the right (`optA ?? optB`). This mirrors the zippy Semigroupal product
// used by the other PublisherT* combinators. It is deterministic but NOT a lawful Alternative:
// zip truncates at the shorter side, so the right-identity law fails for |x| > 1. It is therefore
// exposed as a named function only — there is deliberately no `<|>` operator for it (that operator
// is reserved for lawful monoids like `Publisher.alt` / `DeferredStream.alt`, which use concat).
public func altPublisherOptional<A: Sendable, F: Error>(
    _ lhs: Publisher<A?, F>,
    _ rhs: @autoclosure () -> Publisher<A?, F>
) -> Publisher<A?, F> {
    lhs.zip(rhs()).map { pair in pair.0 ?? pair.1 }
}
