import CoreFP

extension Publisher {
    // Uses _operator so upstream is subscribed synchronously in the DeferredStream factory
    // (same guarantee as all other operators — no hidden inner Task from DeferredStream.map).
    public func map<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            while let result = await upstream.next() {
                switch result {
                case .success(let v):
                    if case .terminated = raw.yield(Result.success(transform(v))) { return }
                case .failure(let e):
                    _ = raw.yield(Result.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func compactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T?
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            while let result = await upstream.next() {
                switch result {
                case .success(let value):
                    if let mapped = transform(value) {
                        if case .terminated = raw.yield(Result.success(mapped)) { return }
                    }
                case .failure(let error):
                    _ = raw.yield(Result.failure(error)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func mapError<E: Error>(
        _ transform: @escaping @Sendable (Failure) -> E
    ) -> Publisher<Output, E> {
        let selfStream = _stream
        return Publisher<Output, E>(DeferredStream {
            let upstream = _StreamBox<Result<Output, Failure>>(selfStream)
            return AsyncStream<Result<Output, E>> { raw in
                let task = Task {
                    while let result = await upstream.next() {
                        switch result {
                        case .success(let v):
                            if case .terminated = raw.yield(Result.success(v)) { return }
                        case .failure(let e):
                            _ = raw.yield(Result.failure(transform(e))); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

// tryMap — sole exception to the non-throwing closure rule.
// Uses Swift typed throws to preserve the error type rather than erasing to any Error.
extension Publisher where Failure == Never {
    public func tryMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T
    ) -> Publisher<T, E> {
        Publisher<T, E> { continuation in
            for await result in self._stream {
                if case .success(let value) = result {
                    let mapped = try transform(value)
                    continuation.yield(mapped)
                }
            }
        }
    }
}

extension Publisher {
    public func tryMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        Publisher<T, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let value):
                    let mapped = try transform(value)
                    continuation.yield(mapped)
                case .failure(let error):
                    throw error
                }
            }
        }
    }
}

// MARK: - Operator helper

// Creates a Publisher by synchronously subscribing to self (via _StreamBox) and running
// body in a Task. The box is passed to body so callers iterate it with `upstream.next()`
// rather than re-subscribing via self._stream — ensuring the subscription is registered
// before _operator returns (and thus before any values can be missed).
extension Publisher {
    func _operator<T: Sendable>(
        _ body: @escaping @Sendable (
            AsyncStream<Result<T, Failure>>.Continuation,
            _StreamBox<Result<Output, Failure>>
        ) async -> Void
    ) -> Publisher<T, Failure> {
        let selfStream = _stream
        return Publisher<T, Failure>(DeferredStream {
            let upstream = _StreamBox<Result<Output, Failure>>(selfStream)
            return AsyncStream<Result<T, Failure>> { downstream in
                let task = Task { await body(downstream, upstream) }
                downstream.onTermination = { _ in task.cancel() }
            }
        })
    }
}
