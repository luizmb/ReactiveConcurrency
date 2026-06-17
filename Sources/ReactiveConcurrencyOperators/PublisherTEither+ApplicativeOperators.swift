import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Publisher<Either<l,a->b>, f> -> Publisher<Either<l,a>, f> -> Publisher<Either<l,b>, f>
public func <*> <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Either<L, @Sendable (A) -> B>, F>,
    _ values: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    applyTPublisherEither(fns, values)
}
