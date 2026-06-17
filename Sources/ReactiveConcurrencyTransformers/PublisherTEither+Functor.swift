import DataStructure
import ReactiveConcurrency

// PublisherTEither: outer = Publisher, inner = Either
// Type: Publisher<Either<L, A>, F>  — ExceptT l over a typed-failure Publisher.
// The outer Failure F flows through the Publisher's own channel; the inner Either.left(l)
// short-circuits the transformer.

public func mapTPublisherEither<L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    publisher.map { either in either.mapRight(fn) }
}

public func fmapTPublisherEither<L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<Either<L, A>, F>) -> Publisher<Either<L, B>, F> {
    { @Sendable publisher in mapTPublisherEither(fn, publisher) }
}
