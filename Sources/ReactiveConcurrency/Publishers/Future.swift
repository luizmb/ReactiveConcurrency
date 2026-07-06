// SPDX-License-Identifier: Apache-2.0

// Future: a cold, single-value publisher. Runs its work on each subscription, emits one
// value, then finishes — or fails. Unlike Combine's Future (eager + multicast), this is cold
// to match the rest of the library: every subscription re-runs the work.
//
// Future is the Publisher-shaped complement to DeferredTask; the two convert back and forth
// via DeferredTask.eraseToPublisher() / Publisher.firstValue() (see below).

public extension Publisher {
    // future :: (() async throws(e) -> a) -> Publisher a e
    static func future(
        _ work: @escaping @Sendable () async throws(Failure) -> Output
    ) -> Publisher<Output, Failure> {
        // Capture the typed throw as a Result inside a non-throwing closure (typed-throws is
        // not inferred through the init's closure), then delegate to the Result-returning form.
        future {
            do throws(Failure) {
                return Result<Output, Failure>.success(try await work())
            } catch {
                return Result<Output, Failure>.failure(error)
            }
        }
    }

    // future :: (() async -> Result a e) -> Publisher a e
    // Result-returning form for callers that already produce a Result rather than throwing.
    static func future(
        _ work: @escaping @Sendable () async -> Result<Output, Failure>
    ) -> Publisher<Output, Failure> {
        Publisher { continuation in
            switch await work() {
            case let .success(value): continuation.yield(value)
            case let .failure(error): continuation.fail(error)
            }
        }
    }
}

// MARK: - DeferredTask <-> Future bridges

public extension DeferredTask {
    // DeferredTask<Success> -> Publisher<Success, Never>: emit the single value, then finish.
    func eraseToPublisher() -> Publisher<Success, Never> {
        Publisher<Success, Never> { continuation in
            continuation.yield(await body())
        }
    }

    // DeferredTask<Result<A, E>> -> Publisher<A, E>: emit the value, or fail with E.
    func eraseToThrowingPublisher<A: Sendable, E: Error>() -> Publisher<A, E>
    where Success == Result<A, E> {
        Publisher<A, E> { continuation in
            switch await body() {
            case let .success(value): continuation.yield(value)
            case let .failure(error): continuation.fail(error)
            }
        }
    }
}

public extension Publisher where Failure == Never {
    /// Awaits the first emitted value, or `nil` if the publisher finishes without emitting:
    /// `let v = await publisher.firstValue()`. Subscribes, takes the first value, and tears down.
    func firstValue() async -> Output? {
        for await result in _stream {
            if case let .success(value) = result { return value }
        }
        return nil
    }

    // Lazy form: the first emitted value as a composable DeferredTask. The inverse of
    // DeferredTask.eraseToPublisher().
    func firstValueTask() -> DeferredTask<Output?> {
        let factory = _stream.factory
        return DeferredTask<Output?> {
            for await result in factory() {
                if case let .success(value) = result { return value }
            }
            return nil
        }
    }
}

public extension Publisher {
    /// Awaits the first event (value or typed failure), or `nil` if the publisher finishes
    /// without emitting: `let r = await publisher.firstResult()`.
    func firstResult() async -> Result<Output, Failure>? {
        for await result in _stream {
            return result
        }
        return nil
    }

    // Lazy form: the first event as a composable DeferredTask.
    func firstResultTask() -> DeferredTask<Result<Output, Failure>?> {
        let factory = _stream.factory
        return DeferredTask<Result<Output, Failure>?> {
            for await result in factory() {
                return result
            }
            return nil
        }
    }
}
