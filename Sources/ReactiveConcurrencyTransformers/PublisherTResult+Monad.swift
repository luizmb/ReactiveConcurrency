import ReactiveConcurrency

// PublisherTResult: outer = Publisher, inner = Result
// Type: Publisher<Result<A, E>, F>

// flatMapT: inner .failure short-circuits (emitted as failure); .success(a) proceeds through fn.
// Sequential (maxPublishers: 1) preserves emission order.
public func flatMapTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ publisher: Publisher<Result<A, E>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> Publisher<Result<B, E>, F> {
    publisher.flatMap(maxPublishers: 1) { result in
        switch result {
        case let .success(a): fn(a)
        case let .failure(e): Publisher<Result<B, E>, F>.just(.failure(e))
        }
    }
}

public func bindTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> @Sendable (Publisher<Result<A, E>, F>) -> Publisher<Result<B, E>, F> {
    { @Sendable publisher in flatMapTPublisherResult(publisher, fn) }
}
