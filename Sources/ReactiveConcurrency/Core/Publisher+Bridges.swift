// Bridges between Publisher and its underlying DeferredStream. A Publisher<Output, Failure>
// is internally a DeferredStream<Result<Output, Failure>>, so these conversions are cheap.

extension DeferredStream {
    // DeferredStream<Element> -> Publisher<Element, Never>: every element is a success.
    public func eraseToPublisher() -> Publisher<Element, Never> {
        Publisher<Element, Never>(map { Result<Element, Never>.success($0) })
    }

    // DeferredStream<Result<A, E>> -> Publisher<A, E>: surface the Result events on the
    // Publisher's value/failure channels. E may be Never. The exact inverse of
    // Publisher.toResultStream().
    public func eraseToThrowingPublisher<A: Sendable, E: Error>() -> Publisher<A, E>
        where Element == Result<A, E> {
        Publisher<A, E>(self)
    }
}

extension Publisher {
    // Full-fidelity DeferredStream of the underlying Result events (value and failure).
    public func toResultStream() -> DeferredStream<Result<Output, Failure>> {
        _stream
    }
}

extension Publisher where Failure == Never {
    // DeferredStream of just the values. Failure == Never means every event is a success,
    // so no failure can be dropped. The inverse of DeferredStream.eraseToPublisher().
    public func toDeferredStream() -> DeferredStream<Output> {
        let factory = _stream.factory
        return DeferredStream<Output> {
            let upstream = factory()
            return AsyncStream<Output> { continuation in
                let task = Task {
                    for await result in upstream {
                        if case .success(let value) = result { continuation.yield(value) }
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }
}
