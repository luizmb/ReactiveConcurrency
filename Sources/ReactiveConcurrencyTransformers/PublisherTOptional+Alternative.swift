// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: positional element-wise choice (NOT a lawful Alternative)
// Type: Publisher<A?, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when present, otherwise the right (`optA ?? optB`).  It is deterministic but NOT a lawful Alternative:
// zip truncates at the shorter side, so the right-identity law fails for |x| > 1. It is therefore
// exposed as `zipAlt` only — there is deliberately no `<|>` operator for it (that operator
// is reserved for lawful monoids like `Publisher.alt` / `DeferredStream.alt`, which use concat).
/// (deterministic but NOT a lawful Alternative; no `<|>`).
/// Positional element-wise choice for the Publisher-over-Optional stack — positional zip taking the left value when present, else the right
func altPublisherOptional<A: Sendable, F: Error>(
    _ lhs: Publisher<A?, F>,
    _ rhs: @autoclosure () -> Publisher<A?, F>
) -> Publisher<A?, F> {
    lhs.zip(rhs()).map { pair in pair.0 ?? pair.1 }
}

// MARK: - Stack surface

public extension PublisherTOptional {
    /// Positional choice: pairs `self` and `other` by index (`zip`) and keeps the left value when it
    /// is present, else the right. Truncates to the shorter side, so it is NOT a lawful Alternative
    /// (no `<|>`).
    /// - Parameter other: The publisher paired with `self`.
    /// - Returns: The element-wise choice.
    func zipAlt(_ other: @autoclosure () -> PublisherTOptional<Failure, A>) -> PublisherTOptional<Failure, A> {
        PublisherTOptional(altPublisherOptional(rawValue, other().rawValue))
    }
}
