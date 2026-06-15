// Publisher<Output, Failure> — a cold, lazy, composable stream backed by DeferredStream.
//
// Internal representation:
//   DeferredStream<Result<Output, Failure>>
//
// For Publisher<Output, Never>, Result<Output, Never> only ever carries .success(Output).
// For Publisher<Output, Failure>, a .failure(e) element is always followed by stream termination.
//
// Nothing executes until a subscriber attaches (cold semantics via DeferredStream).
public struct Publisher<Output: Sendable, Failure: Error>: Sendable {
    let _stream: DeferredStream<Result<Output, Failure>>

    // Low-level init used by operators — direct DeferredStream wrapping.
    init(_ stream: DeferredStream<Result<Output, Failure>>) {
        _stream = stream
    }

    // High-level init for custom publishers.
    //
    // - The body runs in a Task when a subscriber attaches.
    // - CancellationError from `try Task.checkCancellation()` or any `try await` resolves
    //   to graceful .finished — it never surfaces as .failure to subscribers.
    // - Throwing `Failure` resolves to .failure(error).
    // - Returning normally resolves to .finished.
    public init(_ body: @escaping @Sendable (Continuation) async throws -> Void) {
        _stream = DeferredStream {
            let (stream, raw) = AsyncStream<Result<Output, Failure>>.makeStream()
            let cont = Continuation(raw)
            let task = Task {
                await withTaskCancellationHandler {
                    do {
                        try await body(cont)
                    } catch {
                        if !(error is CancellationError), let failure = error as? Failure {
                            raw.yield(.failure(failure))
                        }
                    }
                    raw.finish()
                } onCancel: {
                    raw.finish()
                }
            }
            raw.onTermination = { _ in task.cancel() }
            return stream
        }
    }
}

// MARK: - Continuation

extension Publisher {
    // Passed to the Publisher.init body. Mirrors AsyncStream.Continuation but:
    //   - yield(_:)         wraps in .success
    //   - fail(_:)          wraps in .failure and seals the stream
    //   - yieldAll(sync)    checks isCancelled between elements — no try needed
    //   - yieldAll(async)   cooperative cancellation propagates through for-await
    //   - suspendUntilCancelled() parks the body when driven by external callbacks
    public struct Continuation: Sendable {
        private let _raw: AsyncStream<Result<Output, Failure>>.Continuation

        init(_ raw: AsyncStream<Result<Output, Failure>>.Continuation) {
            _raw = raw
        }

        public func yield(_ value: Output) {
            _raw.yield(.success(value))
        }

        public func fail(_ error: Failure) {
            _raw.yield(.failure(error))
            _raw.finish()
        }

        public func finish() {
            _raw.finish()
        }

        // True when the Task is cancelled OR the downstream unsubscribed.
        // Use in synchronous loops instead of try Task.checkCancellation().
        public var isCancelled: Bool { Task.isCancelled }

        // Sync sequence: cancellation checked between every element.
        public func yieldAll<S: Sequence>(_ sequence: S) where S.Element == Output {
            for value in sequence {
                guard !isCancelled else { return }
                yield(value)
            }
        }

        // Async sequence: cooperative cancellation ends the for-await automatically.
        // AsyncSequence.Failure is macOS 15+ / iOS 18+; gate accordingly.
        @available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, visionOS 2.0, *)
        public func yieldAll<S: AsyncSequence & Sendable>(_ sequence: S) async
            where S.Element == Output, S.Failure == Never {
            for await value in sequence {
                yield(value)
            }
        }

        // Park the body indefinitely. Use when the publisher is callback-driven
        // (e.g., delegate-based APIs). Returns when the Task is cancelled.
        public func suspendUntilCancelled() async {
            let (stream, cont) = AsyncStream<Void>.makeStream()
            await withTaskCancellationHandler {
                for await _ in stream {}
            } onCancel: {
                cont.finish()
            }
        }
    }
}

// MARK: - Type erasure

public typealias AnyPublisher<Output: Sendable, Failure: Error> = Publisher<Output, Failure>

extension Publisher {
    // Publisher is already type-erased; this is a no-op for API compatibility.
    public func eraseToAnyPublisher() -> Publisher<Output, Failure> { self }
}

// MARK: - Convenience constructors

extension Publisher {
    public static func just(_ value: Output) -> Publisher<Output, Failure> {
        Publisher { continuation in
            continuation.yield(value)
        }
    }

    public static func empty() -> Publisher<Output, Failure> {
        Publisher { _ in }
    }
}

extension Publisher where Failure == Never {
    public static func sequence<S: Sequence & Sendable>(_ sequence: S) -> Publisher<Output, Never>
        where S.Element == Output {
        Publisher { continuation in
            continuation.yieldAll(sequence)
        }
    }
}

extension Publisher {
    public static func fail(_ error: Failure) -> Publisher<Output, Failure> {
        Publisher { continuation in
            continuation.fail(error)
        }
    }
}
