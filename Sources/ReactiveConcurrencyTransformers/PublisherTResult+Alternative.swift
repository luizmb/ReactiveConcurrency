// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTResult: zippy element-wise choice (NOT a lawful Alternative)
// Type: Publisher<Result<A, E>, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when it is .success, otherwise the right. If both are .failure the right (last) failure is
// kept. Mirrors the zippy Semigroupal product used by the other PublisherT* combinators. It is
// deterministic but NOT a lawful Alternative: zip truncates at the shorter side, so right identity
// fails for |x| > 1. Exposed as a named function only — no `<|>` operator (reserved for lawful
// monoids like `Publisher.alt` / `DeferredStream.alt`, which use concat).
public func altPublisherResult<A: Sendable, E: Error & Sendable, F: Error>(
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
