import ReactiveConcurrency

// PublisherTResult: Alternative
// Type: Publisher<Result<A, E>, F>
//
// altT pairs the two streams positionally (via zip) and, for each pair, takes the left value
// when it is .success, otherwise the right. If both are .failure the right (last) failure is
// kept. Mirrors the zip-based applicative used by the other PublisherT* combinators.
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
