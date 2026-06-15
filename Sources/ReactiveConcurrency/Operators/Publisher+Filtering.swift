// MARK: - first / last

extension Publisher {
    public func first() -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    _ = raw.yield(.success(v)); raw.finish(); return
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func first(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        filter(predicate).first()
    }

    public func last() -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var lastValue: Output?
            for await result in upstream {
                switch result {
                case .success(let v): lastValue = v
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if let v = lastValue { _ = raw.yield(.success(v)) }
            raw.finish()
        }
    }

    public func last(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        filter(predicate).last()
    }
}

// MARK: - prefix / dropFirst / drop(while:) / prefix(while:)

extension Publisher {
    public func prefix(_ maxLength: Int) -> Publisher<Output, Failure> {
        guard maxLength > 0 else { return .empty() }
        return _operator { raw, upstream in
            var emitted = 0
            for await result in upstream {
                switch result {
                case .success(let v):
                    if case .terminated = raw.yield(.success(v)) { return }
                    emitted += 1
                    if emitted >= maxLength { raw.finish(); return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func prefix(while predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    guard predicate(v) else { raw.finish(); return }
                    if case .terminated = raw.yield(.success(v)) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func dropFirst(_ count: Int = 1) -> Publisher<Output, Failure> {
        guard count > 0 else { return self }
        return _operator { raw, upstream in
            var dropped = 0
            for await result in upstream {
                switch result {
                case .success(let v):
                    if dropped < count { dropped += 1; continue }
                    if case .terminated = raw.yield(.success(v)) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func drop(while predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var dropping = true
            for await result in upstream {
                switch result {
                case .success(let v):
                    if dropping {
                        if predicate(v) { continue }
                        dropping = false
                    }
                    if case .terminated = raw.yield(.success(v)) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }
}

// MARK: - output(at:) / output(in:)

extension Publisher {
    public func output(at index: Int) -> Publisher<Output, Failure> {
        dropFirst(index).first()
    }

    public func output(in range: Range<Int>) -> Publisher<Output, Failure> {
        guard !range.isEmpty else { return .empty() }
        return dropFirst(range.lowerBound).prefix(range.count)
    }
}

// MARK: - ignoreOutput

extension Publisher {
    public func ignoreOutput() -> Publisher<Never, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Never, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Never, Failure>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success: continue
                        case .failure(let e):
                            _ = raw.yield(.failure(e)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

// MARK: - removeDuplicates

extension Publisher where Output: Equatable {
    public func removeDuplicates() -> Publisher<Output, Failure> {
        removeDuplicates(by: ==)
    }
}

extension Publisher {
    public func removeDuplicates(
        by predicate: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var last: Output?
            for await result in upstream {
                switch result {
                case .success(let v):
                    if let l = last, predicate(l, v) { continue }
                    last = v
                    if case .terminated = raw.yield(.success(v)) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }
}

// MARK: - contains / allSatisfy

extension Publisher where Output: Equatable {
    public func contains(_ value: Output) -> Publisher<Bool, Failure> {
        contains(where: { $0 == value })
    }
}

extension Publisher {
    public func contains(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Bool, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case .success(let v):
                    if predicate(v) {
                        _ = raw.yield(.success(true)); raw.finish(); return
                    }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            _ = raw.yield(.success(false))
            raw.finish()
        }
    }

    public func allSatisfy(_ predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Bool, Failure> {
        contains(where: { !predicate($0) }).map { !$0 }
    }
}

// MARK: - min / max

extension Publisher where Output: Comparable {
    public func min() -> Publisher<Output, Failure> {
        _extremum(isLess: { @Sendable a, b in a < b })
    }

    public func max() -> Publisher<Output, Failure> {
        _extremum(isLess: { @Sendable a, b in a > b })
    }
}

extension Publisher {
    public func min(
        by areInIncreasingOrder: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _extremum(isLess: areInIncreasingOrder)
    }

    public func max(
        by areInIncreasingOrder: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _extremum(isLess: { areInIncreasingOrder($1, $0) })
    }

    // Emits `output` if the upstream completes without having produced any values.
    public func replaceEmpty(with output: Output) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var emitted = false
            for await result in upstream {
                switch result {
                case .success(let v):
                    emitted = true
                    if case .terminated = raw.yield(.success(v)) { return }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if !emitted { _ = raw.yield(.success(output)) }
            raw.finish()
        }
    }

    private func _extremum(
        isLess: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var best: Output?
            for await result in upstream {
                switch result {
                case .success(let v):
                    if let b = best { if isLess(v, b) { best = v } } else { best = v }
                case .failure(let e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if let b = best { _ = raw.yield(.success(b)) }
            raw.finish()
        }
    }
}
