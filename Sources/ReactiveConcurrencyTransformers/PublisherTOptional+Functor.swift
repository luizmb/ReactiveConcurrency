import ReactiveConcurrency

// PublisherTOptional: outer = Publisher, inner = Optional
// Type: Publisher<A?, F>  — MaybeT over a typed-failure Publisher.

public func mapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<A?, F>
) -> Publisher<B?, F> {
    publisher.map { optA in optA.map(fn) }
}

public func fmapTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<A?, F>) -> Publisher<B?, F> {
    { @Sendable publisher in mapTPublisherOptional(fn, publisher) }
}
