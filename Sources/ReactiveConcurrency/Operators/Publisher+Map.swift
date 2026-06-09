extension Publisher {
    public func map<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    if case .terminated = raw.yield(.success(transform(v))) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func compactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T?
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case .success(let value):
                    if let mapped = transform(value) {
                        if case .terminated = raw.yield(.success(mapped)) { return }
                    }
                case .failure(let error):
                    _ = raw.yield(.failure(error)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func mapError<E: Error>(
        _ transform: @escaping @Sendable (Failure) -> E
    ) -> Publisher<Output, E> {
        let selfFactory = _stream.factory
        return Publisher<Output, E>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, E>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success(let v):
                            if case .terminated = raw.yield(.success(v)) { return }
                        case .failure(let e):
                            _ = raw.yield(.failure(transform(e))); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

// MARK: - setFailureType / replaceNil

extension Publisher where Failure == Never {
    public func setFailureType<E: Error>(to failureType: E.Type) -> Publisher<Output, E> {
        let selfFactory = _stream.factory
        return Publisher<Output, E>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, E>> { raw in
                let task = Task {
                    for await result in upstream {
                        if case .success(let v) = result {
                            if case .terminated = raw.yield(.success(v)) { return }
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

extension Publisher {
    public func replaceNil<T: Sendable>(with value: T) -> Publisher<T, Failure> where Output == T? {
        map { $0 ?? value }
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

// Calls self._stream.factory() synchronously so the upstream subscription is registered
// before the DeferredStream factory returns — values sent after _operator returns cannot
// be missed. The resulting AsyncStream is passed to body for iteration.
extension Publisher {
    func _operator<T: Sendable>(
        _ body: @escaping @Sendable (
            AsyncStream<Result<T, Failure>>.Continuation,
            AsyncStream<Result<Output, Failure>>
        ) async -> Void
    ) -> Publisher<T, Failure> {
        let selfFactory = _stream.factory
        return Publisher<T, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<T, Failure>> { downstream in
                let task = Task { await body(downstream, upstream) }
                downstream.onTermination = { _ in task.cancel() }
            }
        })
    }
}
