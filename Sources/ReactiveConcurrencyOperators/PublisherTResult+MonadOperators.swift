import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Result<a,e>, f> -> (a -> Publisher<Result<b,e>, f>) -> Publisher<Result<b,e>, f>
public func >>- <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ publisher: Publisher<Result<A, E>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> Publisher<Result<B, E>, F> {
    flatMapTPublisherResult(publisher, fn)
}

// (-<<) :: (a -> Publisher<Result<b,e>, f>) -> Publisher<Result<a,e>, f> -> Publisher<Result<b,e>, f>
public func -<< <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>,
    _ publisher: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    publisher >>- fn
}
