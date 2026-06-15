// Try variants follow the same two-overload pattern as tryMap:
//   • Failure == Never  → closure introduces a new typed error E  (_tryOperator)
//   • Failure != Never  → closure throws the same Failure type    (_operator)

// MARK: - tryFilter

extension Publisher where Failure == Never {
    public func tryFilter<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let value) = result {
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

extension Publisher {
    public func tryFilter(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let value):
                    do throws(Failure) {
                        if try predicate(value) {
                            if case .terminated = downstream.yield(.success(value)) { return }
                        }
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
}

// MARK: - tryCompactMap

extension Publisher where Failure == Never {
    public func tryCompactMap<T: Sendable, E: Error>(
        _ transform: @escaping @Sendable (Output) throws(E) -> T?
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let value) = result {
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

extension Publisher {
    public func tryCompactMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) throws(Failure) -> T?
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let value):
                    do throws(Failure) {
                        if let mapped = try transform(value) {
                            if case .terminated = downstream.yield(.success(mapped)) { return }
                        }
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
}

// MARK: - tryScan

extension Publisher where Failure == Never {
    public func tryScan<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                if case .success(let value) = result {
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

extension Publisher {
    public func tryScan<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case .success(let value):
                    do throws(Failure) {
                        acc = try next(acc, value)
                        if case .terminated = downstream.yield(.success(acc)) { return }
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
}

// MARK: - tryReduce

extension Publisher where Failure == Never {
    public func tryReduce<T: Sendable, E: Error>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(E) -> T
    ) -> Publisher<T, E> {
        _tryOperator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                if case .success(let value) = result {
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

extension Publisher {
    public func tryReduce<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) throws(Failure) -> T
    ) -> Publisher<T, Failure> {
        _operator { downstream, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case .success(let value):
                    do throws(Failure) {
                        acc = try next(acc, value)
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(acc)) { return }
            downstream.finish()
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
        _tryOperator { downstream, upstream in
            var dropping = true
            for await result in upstream {
                if case .success(let v) = result {
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

    public func tryPrefix<E: Error>(
        while predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let v) = result {
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

extension Publisher {
    public func tryDrop(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            var dropping = true
            for await result in upstream {
                switch result {
                case .success(let v):
                    if dropping {
                        do throws(Failure) {
                            if try predicate(v) { continue }
                            dropping = false
                        } catch {
                            _ = downstream.yield(.failure(error)); downstream.finish(); return
                        }
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }

    public func tryPrefix(
        while predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    do throws(Failure) {
                        guard try predicate(v) else { downstream.finish(); return }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    if case .terminated = downstream.yield(.success(v)) { return }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }
}

// MARK: - tryContains / tryAllSatisfy / tryRemoveDuplicates

extension Publisher where Failure == Never {
    public func tryContains<E: Error>(
        where predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let v) = result {
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

    public func tryAllSatisfy<E: Error>(
        _ predicate: @escaping @Sendable (Output) throws(E) -> Bool
    ) -> Publisher<Bool, E> {
        _tryOperator { downstream, upstream in
            for await result in upstream {
                if case .success(let v) = result {
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

    public func tryRemoveDuplicates<E: Error>(
        by predicate: @escaping @Sendable (Output, Output) throws(E) -> Bool
    ) -> Publisher<Output, E> {
        _tryOperator { downstream, upstream in
            var last: Output? = nil
            for await result in upstream {
                if case .success(let v) = result {
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

extension Publisher {
    public func tryContains(
        where predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    do throws(Failure) {
                        if try predicate(v) {
                            if case .terminated = downstream.yield(.success(true)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(false)) { return }
            downstream.finish()
        }
    }

    public func tryAllSatisfy(
        _ predicate: @escaping @Sendable (Output) throws(Failure) -> Bool
    ) -> Publisher<Bool, Failure> {
        _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    do throws(Failure) {
                        if !(try predicate(v)) {
                            if case .terminated = downstream.yield(.success(false)) { return }
                            downstream.finish(); return
                        }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            if case .terminated = downstream.yield(.success(true)) { return }
            downstream.finish()
        }
    }

    public func tryRemoveDuplicates(
        by predicate: @escaping @Sendable (Output, Output) throws(Failure) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { downstream, upstream in
            var last: Output? = nil
            for await result in upstream {
                switch result {
                case .success(let v):
                    do throws(Failure) {
                        if let l = last, try predicate(l, v) { continue }
                    } catch {
                        _ = downstream.yield(.failure(error)); downstream.finish(); return
                    }
                    last = v
                    if case .terminated = downstream.yield(.success(v)) { return }
                case .failure(let e):
                    _ = downstream.yield(.failure(e)); downstream.finish(); return
                }
            }
            downstream.finish()
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
                            let outcome: Result<Publisher<Output, E>, E> = {
                                do throws(E) { return .success(try handler(e)) }
                                catch { return .failure(error) }
                            }()
                            switch outcome {
                            case .success(let recovery):
                                for await r in recovery._stream.factory() {
                                    if case .terminated = raw.yield(r) { return }
                                    if case .failure = r { raw.finish(); return }
                                }
                            case .failure(let typedError):
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
