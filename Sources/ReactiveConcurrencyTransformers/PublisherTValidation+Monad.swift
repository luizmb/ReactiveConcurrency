import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>
// Monad is sequential and short-circuits on .failure — use the Applicative for error accumulation.
public func flatMapTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Validation<E, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Validation<E, B>, F>
) -> Publisher<Validation<E, B>, F> {
    publisher.flatMap(maxPublishers: 1) { v in
        switch v {
        case let .success(a): fn(a)
        case let .failure(e): Publisher<Validation<E, B>, F>.just(.failure(e))
        }
    }
}

public func bindTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Validation<E, B>, F>
) -> @Sendable (Publisher<Validation<E, A>, F>) -> Publisher<Validation<E, B>, F> {
    { @Sendable publisher in flatMapTPublisherValidation(publisher, fn) }
}
