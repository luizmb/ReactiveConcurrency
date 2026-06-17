import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Publisher<Result<a->b,e>, f> -> Publisher<Result<a,e>, f> -> Publisher<Result<b,e>, f>
public func <*> <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fns: Publisher<Result<@Sendable (A) -> B, E>, F>,
    _ values: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    applyTPublisherResult(fns, values)
}
