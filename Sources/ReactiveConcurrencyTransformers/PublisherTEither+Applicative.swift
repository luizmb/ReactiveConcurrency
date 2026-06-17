import DataStructure
import ReactiveConcurrency

// PublisherTEither: outer = Publisher, inner = Either
// Type: Publisher<Either<L, A>, F>

public func liftA2TPublisherEither<L: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Either<L, A>, F>, Publisher<Either<L, B>, F>) -> Publisher<Either<L, C>, F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair -> Either<L, C> in
            switch (pair.0, pair.1) {
            case let (.right(a), .right(b)): .right(fn(a, b))
            case let (.left(l), _): .left(l)
            case let (_, .left(l)): .left(l)
            }
        }
    }
}

public func applyTPublisherEither<L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Either<L, @Sendable (A) -> B>, F>,
    _ values: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    fns.zip(values).map { pair -> Either<L, B> in
        switch (pair.0, pair.1) {
        case let (.right(f), .right(a)): .right(f(a))
        case let (.left(l), _): .left(l)
        case let (_, .left(l)): .left(l)
        }
    }
}
