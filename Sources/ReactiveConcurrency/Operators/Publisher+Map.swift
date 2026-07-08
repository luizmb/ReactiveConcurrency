// SPDX-License-Identifier: Apache-2.0

import Foundation

public extension Publisher {
    /// Transforms each element from the upstream publisher with the provided closure.
    /// - Parameter transform: A closure that maps an upstream element to a new value.
    /// - Returns: A publisher that emits the transformed elements.
    func map<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    if case .terminated = raw.yield(.success(transform(v))) { return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Transforms each element with the provided closure and republishes only the non-`nil` results.
    /// - Parameter transform: A closure that maps an upstream element to an optional value; `nil` results are dropped.
    /// - Returns: A publisher that emits the unwrapped, non-`nil` transformed elements.
    func compactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T?
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    if let mapped = transform(value) {
                        if case .terminated = raw.yield(.success(mapped)) { return }
                    }
                case let .failure(error):
                    _ = raw.yield(.failure(error)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Converts any failure from the upstream publisher into a new error type.
    /// - Parameter transform: A closure that maps the upstream failure to a new error.
    /// - Returns: A publisher that fails with the transformed error.
    func mapError<E: Error>(
        _ transform: @escaping @Sendable (Failure) -> E
    ) -> Publisher<Output, E> {
        let selfFactory = _stream.factory
        return Publisher<Output, E>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, E>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case let .success(v):
                            if case .terminated = raw.yield(.success(v)) { return }
                        case let .failure(e):
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

public extension Publisher {
    /// Republishes the value at the given key path of each upstream element.
    /// - Parameter keyPath: The key path of the property to publish.
    /// - Returns: A publisher that emits the key-path value of each element.
    func map<T: Sendable>(_ keyPath: KeyPath<Output, T> & Sendable) -> Publisher<T, Failure> {
        map { $0[keyPath: keyPath] }
    }

    /// Republishes the values at the two given key paths of each upstream element as a tuple.
    /// - Returns: A publisher that emits a tuple of the two key-path values.
    func map<T: Sendable, U: Sendable>(
        _ kp1: KeyPath<Output, T> & Sendable,
        _ kp2: KeyPath<Output, U> & Sendable
    ) -> Publisher<(T, U), Failure> {
        map { ($0[keyPath: kp1], $0[keyPath: kp2]) }
    }

    /// Republishes the values at the three given key paths of each upstream element as a tuple.
    /// - Returns: A publisher that emits a tuple of the three key-path values.
    func map<T: Sendable, U: Sendable, V: Sendable>(
        _ kp1: KeyPath<Output, T> & Sendable,
        _ kp2: KeyPath<Output, U> & Sendable,
        _ kp3: KeyPath<Output, V> & Sendable
    ) -> Publisher<(T, U, V), Failure> {
        map { ($0[keyPath: kp1], $0[keyPath: kp2], $0[keyPath: kp3]) }
    }
}

// MARK: - setFailureType / replaceNil

public extension Publisher where Failure == Never {
    /// Reinterprets the failure type of a publisher that can never fail.
    /// - Parameter failureType: The error type to adopt.
    /// - Returns: A publisher with the specified failure type that still never fails.
    func setFailureType<E: Error>(to failureType: E.Type) -> Publisher<Output, E> {
        let selfFactory = _stream.factory
        return Publisher<Output, E>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, E>> { raw in
                let task = Task {
                    for await result in upstream {
                        if case let .success(v) = result {
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

public extension Publisher {
    /// Replaces `nil` elements from an optional-valued upstream with the provided value.
    /// - Parameter value: The value to emit in place of `nil`.
    /// - Returns: A publisher that emits non-optional elements.
    func replaceNil<T: Sendable>(with value: T) -> Publisher<T, Failure> where Output == T? {
        map { $0 ?? value }
    }
}

// MARK: - tryMap

// Throwing variants use Swift typed throws so the error type is preserved.
// Result-returning variants are the non-throwing equivalent — useful when the
// transform already produces a Result (e.g. a decoder) rather than throwing.

public extension Publisher where Failure == Never {
    /// Transforms each element with a throwing closure, using typed throws to preserve the error type.
    /// - Parameter transform: A closure that maps an upstream element and may throw a typed error `E`.
    /// - Returns: A publisher that fails with `E` if the transform throws.
    func tryMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(value) = result {
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

    /// Transforms each element with a `Result`-returning closure; the non-throwing equivalent of `tryMap`.
    /// - Parameter transform: A closure that maps an upstream element to a `Result`; a `.failure` fails the publisher.
    /// - Returns: A publisher that fails with `E` on the first `.failure`.
    func tryMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) -> Result<T, E>
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(value) = result {
                    switch transform(value) {
                    case let .success(mapped):
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    case let .failure(e):
                        _ = downstream.yield(.failure(e)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Transforms each element with a throwing closure whose thrown error matches the upstream `Failure`.
    /// - Parameter transform: A closure that maps an upstream element and may throw `Failure`.
    /// - Returns: A publisher that fails with `Failure` if the transform throws.
    func tryMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    do throws(Failure) {
                        let mapped = try transform(value)
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }

    /// Transforms each element with a `Result`-returning closure sharing the upstream `Failure` type.
    /// - Parameter transform: A closure that maps an upstream element to a `Result`; a `.failure` fails the publisher.
    /// - Returns: A publisher that fails with `Failure` on the first `.failure`.
    func tryMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Result<T, Failure>
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    switch transform(value) {
                    case let .success(mapped):
                        if case .terminated = downstream.yield(.success(mapped)) { return }
                    case let .failure(e):
                        _ = downstream.yield(.failure(e)); downstream.finish(); return
                    }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}

// MARK: - encode / decode

public extension Publisher where Failure == Never {
    /// Encodes each element to `Data` using the provided encoder, failing with `E` on error.
    /// - Parameter encoder: A closure that encodes an element to `Data` or returns a `.failure`.
    /// - Returns: A publisher of encoded `Data` that fails with `E`.
    func encode<E: Error>(
        encoder: @escaping @Sendable (Output) -> Result<Data, E>
    ) -> Publisher<Data, E> {
        tryMap(encoder)
    }
}

public extension Publisher {
    /// Encodes each element to `Data` using the provided encoder, failing with the upstream `Failure`.
    /// - Parameter encoder: A closure that encodes an element to `Data` or returns a `.failure`.
    /// - Returns: A publisher of encoded `Data`.
    func encode(
        encoder: @escaping @Sendable (Output) -> Result<Data, Failure>
    ) -> Publisher<Data, Failure> {
        tryMap(encoder)
    }
}

public extension Publisher where Output == Data, Failure == Never {
    /// Decodes each `Data` element into a value using the provided decoder, failing with `E` on error.
    /// - Parameter decoder: A closure that decodes `Data` into a value or returns a `.failure`.
    /// - Returns: A publisher of decoded values that fails with `E`.
    func decode<T: Sendable, E: Error>(
        decoder: @escaping @Sendable (Data) -> Result<T, E>
    ) -> Publisher<T, E> {
        tryMap(decoder)
    }
}

public extension Publisher where Output == Data {
    /// Decodes each `Data` element into a value using the provided decoder, failing with the upstream `Failure`.
    /// - Parameter decoder: A closure that decodes `Data` into a value or returns a `.failure`.
    /// - Returns: A publisher of decoded values.
    func decode<T: Sendable>(
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
    /// Low-level building block: pre-subscribes upstream and drives `body` to produce a derived publisher.
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
    /// Low-level building block: like `_operator` but introduces a new typed error `E` from an infallible upstream.
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
