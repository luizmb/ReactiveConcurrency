import DataStructure
import ReactiveConcurrency

// PublisherTEither: outer = Publisher, inner = Either
// Type: Publisher<Either<L, A>, F>

// flatMapT: .left propagates; .right(a) proceeds through fn. Sequential (maxPublishers: 1)
// preserves element order, matching the DeferredStream transformer's concat semantics.
public func flatMapTPublisherEither<L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Either<L, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>
) -> Publisher<Either<L, B>, F> {
    publisher.flatMap(maxPublishers: 1) { either in
        switch either {
        case let .right(a): fn(a)
        case let .left(l): Publisher<Either<L, B>, F>.just(.left(l))
        }
    }
}

public func bindTPublisherEither<L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>
) -> @Sendable (Publisher<Either<L, A>, F>) -> Publisher<Either<L, B>, F> {
    { @Sendable publisher in flatMapTPublisherEither(publisher, fn) }
}
