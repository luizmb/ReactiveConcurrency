import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTValidation: outer = Publisher, inner = Validation
// Type: Publisher<Validation<E, A>, F>. Applicative accumulates errors (Validation's key property).

public func liftA2TPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Validation<E, A>, F>, Publisher<Validation<E, B>, F>) -> Publisher<Validation<E, C>, F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair in Validation<E, C>.liftA2(fn)(pair.0, pair.1) }
    }
}

public func applyTPublisherValidation<E: Semigroup & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<Validation<E, @Sendable (A) -> B>, F>,
    _ values: Publisher<Validation<E, A>, F>
) -> Publisher<Validation<E, B>, F> {
    fns.zip(values).map { pair -> Validation<E, B> in
        switch (pair.0, pair.1) {
        case let (.success(f), .success(a)): .success(f(a))
        case let (.failure(e), .success): .failure(e)
        case let (.success, .failure(e)): .failure(e)
        case let (.failure(e1), .failure(e2)): .failure(E.combine(e1, e2))
        }
    }
}
