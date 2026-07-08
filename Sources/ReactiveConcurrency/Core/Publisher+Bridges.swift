// SPDX-License-Identifier: Apache-2.0

// Bridges between Publisher and its underlying DeferredStream. A Publisher<Output, Failure>
// is internally a DeferredStream<Result<Output, Failure>>, so these conversions are cheap.

public extension DeferredStream {
    /// Bridges this `DeferredStream` into an infallible `Publisher<Element, Never>` where every
    /// element becomes a success.
    func eraseToPublisher() -> Publisher<Element, Never> {
        Publisher<Element, Never>(map { Result<Element, Never>.success($0) })
    }

    /// Bridges a `DeferredStream` of `Result` into a `Publisher<A, E>`, surfacing each `Result` on
    /// the publisher's value/failure channels (`E` may be `Never`). The exact inverse of
    /// `Publisher.results`.
    func eraseToThrowingPublisher<A: Sendable, E: Error>() -> Publisher<A, E>
    where Element == Result<A, E> {
        Publisher<A, E>(self)
    }
}

public extension Publisher {
    /// The underlying events as an `AsyncSequence` of `Result` (value or typed failure):
    /// `for await result in publisher.results { ... }`. Available for any `Failure`; the typed
    /// error is preserved as a value, so iteration never surfaces an untyped `any Error`.
    var results: DeferredStream<Result<Output, Failure>> {
        _stream
    }
}

public extension Publisher where Failure == Never {
    /// The emitted values as an `AsyncSequence`: `for await value in publisher.values { ... }`.
    /// Only available when `Failure == Never` (every event is a success); for failable
    /// publishers use `results`. The inverse of `DeferredStream.eraseToPublisher()`.
    var values: DeferredStream<Output> {
        let factory = _stream.factory
        return DeferredStream<Output> {
            let upstream = factory()
            return AsyncStream<Output> { continuation in
                let task = Task {
                    for await result in upstream {
                        if case let .success(value) = result { continuation.yield(value) }
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }
}

// MARK: - AsyncStream -> Publisher

public extension AsyncStream where Element: Sendable {
    /// Bridges an existing `AsyncStream` into a `Publisher<Element, Never>`.
    ///
    /// The stream is consumed once — an `AsyncStream` cannot be restarted, so only the first
    /// subscription receives elements. For a cold, restartable publisher, build a
    /// `DeferredStream { ... }` and call its `eraseToPublisher()` instead.
    func eraseToPublisher() -> Publisher<Element, Never> {
        DeferredStream.wrap(self).eraseToPublisher()
    }
}
