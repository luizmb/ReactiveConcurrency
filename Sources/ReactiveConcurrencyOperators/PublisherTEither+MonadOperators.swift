import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Either<l,a>, f> -> (a -> Publisher<Either<l,b>, f>) -> Publisher<Either<l,b>, f>
public func >>- <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Either<L, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>
) -> Publisher<Either<L, B>, F> {
    flatMapTPublisherEither(publisher, fn)
}

// (-<<) :: (a -> Publisher<Either<l,b>, f>) -> Publisher<Either<l,a>, f> -> Publisher<Either<l,b>, f>
public func -<< <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>,
    _ publisher: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    publisher >>- fn
}
