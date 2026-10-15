// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTResult: positional element-wise choice (NOT a lawful Alternative)
// Type: Publisher<Result<A, E>, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when it is .success, otherwise the right. If both are .failure the right (last) failure is
// kept.  It is
// deterministic but NOT a lawful Alternative: zip truncates at the shorter side, so right identity
// fails for |x| > 1. Exposed as `zipAlt` only — no `<|>` operator (reserved for lawful
// monoids like `Publisher.alt` / `DeferredStream.alt`, which use concat).
/// (deterministic but NOT a lawful Alternative; no `<|>`).
/// Positional element-wise choice for the Publisher-over-Result stack — positional zip taking the left value when .success, else the right
func altPublisherResult<A: Sendable, E: Error & Sendable, F: Error>(
    _ lhs: Publisher<Result<A, E>, F>,
    _ rhs: @autoclosure () -> Publisher<Result<A, E>, F>
) -> Publisher<Result<A, E>, F> {
    lhs.zip(rhs()).map { pair in
        switch pair.0 {
        case .success: pair.0
        case .failure: pair.1
        }
    }
}

// MARK: - Stack surface

public extension PublisherTResult {
    /// Positional choice: pairs `self` and `other` by index (`zip`) and keeps the left value when it
    /// is .success, else the right. Truncates to the shorter side, so it is NOT a lawful Alternative
    /// (no `<|>`).
    /// - Parameter other: The publisher paired with `self`.
    /// - Returns: The element-wise choice.
    func zipAlt(_ other: @autoclosure () -> PublisherTResult<Failure, E, A>) -> PublisherTResult<Failure, E, A> {
        PublisherTResult(altPublisherResult(rawValue, other().rawValue))
    }
}
