// Try variants follow the same two-overload pattern as tryMap:
//   • Failure == Never  → closure introduces a new typed error E
//   • Failure != Never  → closure throws the same Failure type (no erasure)

// MARK: - tryFilter

extension Publisher where Failure == Never {
    public func tryFilter<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        Publisher<Output, E> { continuation in
            for await result in self._stream {
                if case .success(let value) = result {
                    if try predicate(value) { continuation.yield(value) }
                }
            }
        }
    }
}

extension Publisher {
    public func tryFilter(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        Publisher<Output, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let value):
                    if try predicate(value) { continuation.yield(value) }
                case .failure(let error):
                    throw error
                }
            }
        }
    }
}

// MARK: - tryCompactMap

extension Publisher where Failure == Never {
    public func tryCompactMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T?
    ) -> Publisher<T, E> {
        Publisher<T, E> { continuation in
            for await result in self._stream {
                if case .success(let value) = result {
                    if let mapped = try transform(value) { continuation.yield(mapped) }
                }
            }
        }
    }
}

extension Publisher {
    public func tryCompactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T?
    ) -> Publisher<T, Failure> {
        Publisher<T, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let value):
                    if let mapped = try transform(value) { continuation.yield(mapped) }
                case .failure(let error):
                    throw error
                }
            }
        }
    }
}

// MARK: - tryScan

extension Publisher where Failure == Never {
    public func tryScan<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        Publisher<T, E> { continuation in
            var acc = initial
            for await result in self._stream {
                if case .success(let value) = result {
                    acc = try next(acc, value)
                    continuation.yield(acc)
                }
            }
        }
    }
}

extension Publisher {
    public func tryScan<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        Publisher<T, Failure> { continuation in
            var acc = initial
            for await result in self._stream {
                switch result {
                case .success(let value):
                    acc = try next(acc, value)
                    continuation.yield(acc)
                case .failure(let error):
                    throw error
                }
            }
        }
    }
}

// MARK: - tryReduce

extension Publisher where Failure == Never {
    public func tryReduce<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        Publisher<T, E> { continuation in
            var acc = initial
            for await result in self._stream {
                if case .success(let value) = result {
                    acc = try next(acc, value)
                }
            }
            continuation.yield(acc)
        }
    }
}

extension Publisher {
    public func tryReduce<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        Publisher<T, Failure> { continuation in
            var acc = initial
            for await result in self._stream {
                switch result {
                case .success(let value):
                    acc = try next(acc, value)
                case .failure(let error):
                    throw error
                }
            }
            continuation.yield(acc)
        }
    }
}

// MARK: - tryFirst / tryLast

extension Publisher where Failure == Never {
    public func tryFirst<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        tryFilter(predicate).first()
    }

    public func tryLast<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        tryFilter(predicate).last()
    }
}

extension Publisher {
    public func tryFirst(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        tryFilter(predicate).first()
    }

    public func tryLast(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        tryFilter(predicate).last()
    }
}

// MARK: - tryDrop / tryPrefix

extension Publisher where Failure == Never {
    public func tryDrop<E: Error>(
        while predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        Publisher<Output, E> { continuation in
            var dropping = true
            for await result in self._stream {
                if case .success(let v) = result {
                    if dropping {
                        if try predicate(v) { continue }
                        dropping = false
                    }
                    continuation.yield(v)
                }
            }
        }
    }

    public func tryPrefix<E: Error>(
        while predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        Publisher<Output, E> { continuation in
            for await result in self._stream {
                if case .success(let v) = result {
                    guard try predicate(v) else { return }
                    continuation.yield(v)
                }
            }
        }
    }
}

extension Publisher {
    public func tryDrop(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        Publisher<Output, Failure> { continuation in
            var dropping = true
            for await result in self._stream {
                switch result {
                case .success(let v):
                    if dropping {
                        if try predicate(v) { continue }
                        dropping = false
                    }
                    continuation.yield(v)
                case .failure(let e): throw e
                }
            }
        }
    }

    public func tryPrefix(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        Publisher<Output, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let v):
                    guard try predicate(v) else { return }
                    continuation.yield(v)
                case .failure(let e): throw e
                }
            }
        }
    }
}

// MARK: - tryContains / tryAllSatisfy / tryRemoveDuplicates

extension Publisher where Failure == Never {
    public func tryContains<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        Publisher<Bool, E> { continuation in
            for await result in self._stream {
                if case .success(let v) = result {
                    if try predicate(v) { continuation.yield(true); return }
                }
            }
            continuation.yield(false)
        }
    }

    public func tryAllSatisfy<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        Publisher<Bool, E> { continuation in
            for await result in self._stream {
                if case .success(let v) = result {
                    if !(try predicate(v)) { continuation.yield(false); return }
                }
            }
            continuation.yield(true)
        }
    }

    public func tryRemoveDuplicates<E: Error>(
        by predicate: @escaping @Sendable (Output, Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        Publisher<Output, E> { continuation in
            var last: Output? = nil
            for await result in self._stream {
                if case .success(let v) = result {
                    if let l = last, try predicate(l, v) { continue }
                    last = v
                    continuation.yield(v)
                }
            }
        }
    }
}

extension Publisher {
    public func tryContains(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        Publisher<Bool, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let v):
                    if try predicate(v) { continuation.yield(true); return }
                case .failure(let e): throw e
                }
            }
            continuation.yield(false)
        }
    }

    public func tryAllSatisfy(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        Publisher<Bool, Failure> { continuation in
            for await result in self._stream {
                switch result {
                case .success(let v):
                    if !(try predicate(v)) { continuation.yield(false); return }
                case .failure(let e): throw e
                }
            }
            continuation.yield(true)
        }
    }

    public func tryRemoveDuplicates(
        by predicate: @escaping @Sendable (Output, Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        Publisher<Output, Failure> { continuation in
            var last: Output? = nil
            for await result in self._stream {
                switch result {
                case .success(let v):
                    if let l = last, try predicate(l, v) { continue }
                    last = v
                    continuation.yield(v)
                case .failure(let e): throw e
                }
            }
        }
    }
}

// MARK: - tryCatch

extension Publisher {
    // Like catch, but the recovery handler can throw E. If it does, E propagates downstream.
    public func tryCatch<E: Error>(
        _ handler: @escaping @Sendable (Failure) throws(E) -> Publisher<Output, E>
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
                            do {
                                let recovery = try handler(e)
                                for await r in recovery._stream.factory() {
                                    if case .terminated = raw.yield(r) { return }
                                    if case .failure = r { raw.finish(); return }
                                }
                            } catch {
                                // typed throws(E) guarantees this cast succeeds
                                if let typedError = error as? E {
                                    _ = raw.yield(.failure(typedError)); raw.finish(); return
                                }
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
