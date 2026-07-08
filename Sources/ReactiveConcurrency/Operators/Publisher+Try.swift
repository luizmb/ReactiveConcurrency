// SPDX-License-Identifier: Apache-2.0

// Try variants follow the same two-overload pattern as tryMap:
//   • Failure == Never  → closure introduces a new typed error E  (_tryOperator)
//   • Failure != Never  → closure throws the same Failure type    (_operator)

// MARK: - tryFilter

public extension Publisher where Failure == Never {
    /// Republishes only elements satisfying a throwing predicate, failing with the typed error `E` if it throws.
    /// - Parameter predicate: A closure returning `true` to keep an element; may throw a typed error `E`.
    /// - Returns: A publisher that fails with `E` if the predicate throws.
    func tryFilter<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(value) = result {
                    do throws(E) {
                        if try predicate(value) {
                            if case .terminated = downstream.yield(.success(value)) { return }
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Republishes only elements satisfying a throwing predicate whose thrown error matches the upstream `Failure`.
    /// - Parameter predicate: A closure returning `true` to keep an element; may throw `Failure`.
    /// - Returns: A publisher that fails with `Failure` if the predicate throws.
    func tryFilter(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    do throws(Failure) {
                        if try predicate(value) {
                            if case .terminated = downstream.yield(.success(value)) { return }
                        }
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
}

// MARK: - tryCompactMap

public extension Publisher where Failure == Never {
    /// Transforms elements with a throwing closure, dropping `nil` results and failing with typed error `E` on throw.
    /// - Parameter transform: A closure mapping an element to an optional value; may throw a typed error `E`.
    /// - Returns: A publisher of unwrapped results that fails with `E` if the transform throws.
    func tryCompactMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T?
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(value) = result {
                    do throws(E) {
                        if let mapped = try transform(value) {
                            if case .terminated = downstream.yield(.success(mapped)) { return }
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Transforms elements with a throwing closure, dropping `nil` results; thrown errors match the upstream `Failure`.
    /// - Parameter transform: A closure mapping an element to an optional value; may throw `Failure`.
    /// - Returns: A publisher of unwrapped results that fails with `Failure` if the transform throws.
    func tryCompactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T?
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    do throws(Failure) {
                        if let mapped = try transform(value) {
                            if case .terminated = downstream.yield(.success(mapped)) { return }
                        }
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
}

// MARK: - tryScan

public extension Publisher where Failure == Never {
    /// Emits the running accumulation from a throwing fold, failing with the typed error `E` if it throws.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A throwing closure folding the accumulator and next element; may throw a typed error `E`.
    /// - Returns: A publisher emitting each intermediate accumulator, failing with `E` on throw.
    func tryScan<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                if case let .success(value) = result {
                    do throws(E) {
                        acc = try next(acc, value)
                        if case .terminated = downstream.yield(.success(acc)) { return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Emits the running accumulation from a throwing fold; thrown errors match the upstream `Failure`.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A throwing closure folding the accumulator and next element; may throw `Failure`.
    /// - Returns: A publisher emitting each intermediate accumulator, failing with `Failure` on throw.
    func tryScan<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case let .success(value):
                    do throws(Failure) {
                        acc = try next(acc, value)
                        if case .terminated = downstream.yield(.success(acc)) { return }
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
}

// MARK: - tryReduce

public extension Publisher where Failure == Never {
    /// Folds all elements with a throwing closure, emitting only the final value and failing with typed error `E`.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A throwing closure folding the accumulator and next element; may throw a typed error `E`.
    /// - Returns: A publisher emitting the single final accumulator, failing with `E` on throw.
    func tryReduce<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                if case let .success(value) = result {
                    do throws(E) {
                        acc = try next(acc, value)
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            if case .terminated = downstream.yield(.success(acc)) { return }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Folds all elements with a throwing closure, emitting only the final value; thrown errors match `Failure`.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A throwing closure folding the accumulator and next element; may throw `Failure`.
    /// - Returns: A publisher emitting the single final accumulator, failing with `Failure` on throw.
    func tryReduce<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case let .success(value):
                    do throws(Failure) {
                        acc = try next(acc, value)
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(acc)) { return }
            downstream.finish()
        }
    }
}

// MARK: - tryFirst / tryLast

public extension Publisher where Failure == Never {
    /// Emits the first element satisfying a throwing predicate, failing with the typed error `E` if it throws.
    /// - Parameter predicate: A closure returning `true` for the sought element; may throw a typed error `E`.
    func tryFirst<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        tryFilter(predicate).first()
    }

    /// Emits the last element satisfying a throwing predicate, failing with the typed error `E` if it throws.
    /// - Parameter predicate: A closure returning `true` for candidate elements; may throw a typed error `E`.
    func tryLast<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        tryFilter(predicate).last()
    }
}

public extension Publisher {
    /// Emits the first element satisfying a throwing predicate; thrown errors match the upstream `Failure`.
    /// - Parameter predicate: A closure returning `true` for the sought element; may throw `Failure`.
    func tryFirst(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        tryFilter(predicate).first()
    }

    /// Emits the last element satisfying a throwing predicate; thrown errors match the upstream `Failure`.
    /// - Parameter predicate: A closure returning `true` for candidate elements; may throw `Failure`.
    func tryLast(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        tryFilter(predicate).last()
    }
}

// MARK: - tryDrop / tryPrefix

public extension Publisher where Failure == Never {
    /// Drops elements while a throwing predicate holds, then republishes the rest; fails with typed error `E` on throw.
    /// - Parameter predicate: A closure returning `true` to keep dropping; may throw a typed error `E`.
    func tryDrop<E: Error>(
        while predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            var dropping = true
            for await result in upstream {
                if case let .success(v) = result {
                    if dropping {
                        do throws(E) {
                            if try predicate(v) { continue }
                            dropping = false
                        } catch {
                            _ = downstream.yield(.failure(error)); downstream.finish(); return
                        }
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                }
            }
            downstream.finish()
        }
    }

    /// Republishes elements while a throwing predicate holds, completing at the first failure; fails with `E` on throw.
    /// - Parameter predicate: A closure returning `true` to continue emitting; may throw a typed error `E`.
    func tryPrefix<E: Error>(
        while predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(v) = result {
                    do throws(E) {
                        guard try predicate(v) else { downstream.finish(); return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Drops elements while a throwing predicate holds, then republishes the rest; thrown errors match `Failure`.
    /// - Parameter predicate: A closure returning `true` to keep dropping; may throw `Failure`.
    func tryDrop(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            var dropping = true
            for await result in upstream {
                switch result {
                case let .success(v):
                    if dropping {
                        do throws(Failure) {
                            if try predicate(v) { continue }
                            dropping = false
                        } catch {
                            _ = downstream.yield(.failure(error)); downstream.finish(); return
                        }
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }

    /// Republishes elements while a throwing predicate holds, completing at the first failure; errors match `Failure`.
    /// - Parameter predicate: A closure returning `true` to continue emitting; may throw `Failure`.
    func tryPrefix(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    do throws(Failure) {
                        guard try predicate(v) else { downstream.finish(); return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}

// MARK: - tryContains / tryAllSatisfy / tryRemoveDuplicates

public extension Publisher where Failure == Never {
    /// Emits `true` as soon as an element satisfies a throwing predicate, else `false`; fails with `E` on throw.
    /// - Parameter predicate: A closure returning `true` for a matching element; may throw a typed error `E`.
    func tryContains<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(v) = result {
                    do throws(E) {
                        if try predicate(v) {
                            if case .terminated = downstream.yield(.success(true)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            if case .terminated = downstream.yield(.success(false)) { return }
            downstream.finish()
        }
    }

    /// Emits whether every element satisfies a throwing predicate (short-circuiting on `false`); fails with `E` on throw.
    /// - Parameter predicate: A closure evaluated against each element; may throw a typed error `E`.
    func tryAllSatisfy<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case let .success(v) = result {
                    do throws(E) {
                        if !(try predicate(v)) {
                            if case .terminated = downstream.yield(.success(false)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                }
            }
            if case .terminated = downstream.yield(.success(true)) { return }
            downstream.finish()
        }
    }

    /// Omits consecutive duplicates as judged by a throwing predicate, failing with the typed error `E` on throw.
    /// - Parameter predicate: A closure returning `true` when two consecutive elements are equal; may throw `E`.
    func tryRemoveDuplicates<E: Error>(
        by predicate: @escaping @Sendable (Output, Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            var last: Output?
            for await result in upstream {
                if case let .success(v) = result {
                    do throws(E) {
                        if let l = last, try predicate(l, v) { continue }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    last = v
                    if case .terminated = downstream.yield(.success(v)) { return }
                }
            }
            downstream.finish()
        }
    }
}

public extension Publisher {
    /// Emits `true` as soon as an element satisfies a throwing predicate, else `false`; errors match `Failure`.
    /// - Parameter predicate: A closure returning `true` for a matching element; may throw `Failure`.
    func tryContains(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    do throws(Failure) {
                        if try predicate(v) {
                            if case .terminated = downstream.yield(.success(true)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(false)) { return }
            downstream.finish()
        }
    }

    /// Emits whether every element satisfies a throwing predicate (short-circuiting on `false`); errors match `Failure`.
    /// - Parameter predicate: A closure evaluated against each element; may throw `Failure`.
    func tryAllSatisfy(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    do throws(Failure) {
                        if !(try predicate(v)) {
                            if case .terminated = downstream.yield(.success(false)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(true)) { return }
            downstream.finish()
        }
    }

    /// Omits consecutive duplicates as judged by a throwing predicate; thrown errors match the upstream `Failure`.
    /// - Parameter predicate: A closure returning `true` when two consecutive elements are equal; may throw `Failure`.
    func tryRemoveDuplicates(
        by predicate: @escaping @Sendable (Output, Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            var last: Output?
            for await result in upstream {
                switch result {
                case let .success(v):
                    do throws(Failure) {
                        if let l = last, try predicate(l, v) { continue }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    last = v
                    if case .terminated = downstream.yield(.success(v)) { return }
                case let .failure(e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}

// MARK: - tryCatch

public extension Publisher {
    /// Recovers from an upstream failure with another publisher, propagating `E` if the handler itself throws.
    ///
    /// Like `catch`, but the recovery handler may throw the typed error `E`.
    /// - Parameter handler: A closure mapping the upstream failure to a recovery publisher; may throw `E`.
    /// - Returns: A publisher that continues with the recovery publisher, or fails with `E`.
    func tryCatch<E: Error>(
        _ handler: @escaping @Sendable (Failure) throws(E) -> Publisher<Output, E>
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
                            let outcome: Result<Publisher<Output, E>, E> = {
                                do throws(E) { return .success(try handler(e)) } catch { return .failure(error) }
                            }()
                            switch outcome {
                            case let .success(recovery):
                                for await r in recovery._stream.factory() {
                                    if case .terminated = raw.yield(r) { return }
                                    if case .failure = r { raw.finish(); return }
                                }
                            case let .failure(typedError):
                                _ = raw.yield(.failure(typedError)); raw.finish(); return
                            }
                            raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
