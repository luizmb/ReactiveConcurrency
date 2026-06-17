import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<[a], f> -> (a -> Publisher<[b], f>) -> Publisher<[b], f>
public func >>- <A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<[A], F>,
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>
) -> Publisher<[B], F> {
    flatMapTPublisherArray(publisher, fn)
}

// (-<<) :: (a -> Publisher<[b], f>) -> Publisher<[a], f> -> Publisher<[b], f>
public func -<< <A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<[B], F>,
    _ publisher: Publisher<[A], F>
) -> Publisher<[B], F> {
    publisher >>- fn
}
