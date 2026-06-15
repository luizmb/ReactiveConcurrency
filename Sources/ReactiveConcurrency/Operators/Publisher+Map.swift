import Foundation

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

// MARK: - KeyPath map overloads

extension Publisher {
    public func map<T: Sendable>(_ keyPath: KeyPath<Output, T> & Sendable) -> Publisher<T, Failure> {
        map { $0[keyPath: keyPath] }
    }

    public func map<T: Sendable, U: Sendable>(
        _ kp1: KeyPath<Output, T> & Sendable,
        _ kp2: KeyPath<Output, U> & Sendable
    ) -> Publisher<(T, U), Failure> {
        map { ($0[keyPath: kp1], $0[keyPath: kp2]) }
    }

    public func map<T: Sendable, U: Sendable, V: Sendable>(
        _ kp1: KeyPath<Output, T> & Sendable,
        _ kp2: KeyPath<Output, U> & Sendable,
        _ kp3: KeyPath<Output, V> & Sendable
    ) -> Publisher<(T, U, V), Failure> {
        map { ($0[keyPath: kp1], $0[keyPath: kp2], $0[keyPath: kp3]) }
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

// MARK: - tryMap

// Throwing variants use Swift typed throws so the error type is preserved.
// Result-returning variants are the non-throwing equivalent — useful when the
// transform already produces a Result (e.g. a decoder) rather than throwing.

extension Publisher where Failure == Never {
    public func tryMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let value) = result {
                    do throws(E) {
                        let mapped = try transform(value)
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }

    public func tryMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) -> Result<T, E>
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let value) = result {
                    switch transform(value) {
                    case .success(let mapped):
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    case .failure(let e):
                        _ = downstream.yield(.failure(e)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }
}

extension Publisher {
    public func tryMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let value):
                    do throws(Failure) {
                        let mapped = try transform(value)
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }

    public func tryMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Result<T, Failure>
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let value):
                    switch transform(value) {
                    case .success(let mapped):
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    case .failure(let e):
                        _ = downstream.yield(.failure(e)); downstream.finish(); return
                    }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}

// MARK: - encode / decode

extension Publisher where Failure == Never {
    public func encode<E: Error>(
        encoder: @escaping @Sendable (Output) -> Result<Data, E>
    ) -> Publisher<Data, E> {
        tryMap(encoder)
    }
}

extension Publisher {
    public func encode(
        encoder: @escaping @Sendable (Output) -> Result<Data, Failure>
    ) -> Publisher<Data, Failure> {
        tryMap(encoder)
    }
}

extension Publisher where Output == Data, Failure == Never {
    public func decode<T: Sendable, E: Error>(
        decoder: @escaping @Sendable (Data) -> Result<T, E>
    ) -> Publisher<T, E> {
        tryMap(decoder)
    }
}

extension Publisher where Output == Data {
    public func decode<T: Sendable>(
        decoder: @escaping @Sendable (Data) -> Result<T, Failure>
    ) -> Publisher<T, Failure> {
        tryMap(decoder)
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

// Variant for operators that introduce a new error type E from an infallible upstream.
// The body is non-throwing; typed errors are captured via do throws(E) { } catch { } inside.
extension Publisher where Failure == Never {
    func _tryOperator<T: Sendable, E: Error>(
        _ body: @escaping @Sendable (
            AsyncStream<Result<T, E>>.Continuation,
            AsyncStream<Result<Output, Never>>
        ) async -> Void
    ) -> Publisher<T, E> {
        let selfFactory = _stream.factory
        return Publisher<T, E>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<T, E>> { downstream in
                let task = Task { await body(downstream, upstream) }
                downstream.onTermination = { _ in task.cancel() }
            }
        })
    }
}
