// SPDX-License-Identifier: Apache-2.0

// MARK: - first / last

public extension Publisher {
    /// Republishes only the first element, then completes.
    func first() -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    _ = raw.yield(.success(v)); raw.finish(); return
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Republishes only the first element that satisfies the predicate, then completes.
    /// - Parameter predicate: A closure returning `true` for the element to emit.
    func first(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        filter(predicate).first()
    }

    /// Republishes only the last element, emitted once the upstream completes.
    func last() -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var lastValue: Output?
            for await result in upstream {
                switch result {
                case let .success(v): lastValue = v
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if let v = lastValue { _ = raw.yield(.success(v)) }
            raw.finish()
        }
    }

    /// Republishes only the last element that satisfies the predicate, emitted once the upstream completes.
    /// - Parameter predicate: A closure returning `true` for candidate elements.
    func last(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        filter(predicate).last()
    }
}

// MARK: - prefix / dropFirst / drop(while:) / prefix(while:)

public extension Publisher {
    /// Republishes at most the first `maxLength` elements, then completes.
    /// - Parameter maxLength: The maximum number of elements to emit; a value of `0` or less emits nothing.
    func prefix(_ maxLength: Int) -> Publisher<Output, Failure> {
        guard maxLength > 0 else { return .empty() }
        return _operator { raw, upstream in
            var emitted = 0
            for await result in upstream {
                switch result {
                case let .success(v):
                    if case .terminated = raw.yield(.success(v)) { return }
                    emitted += 1
                    if emitted >= maxLength { raw.finish(); return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Republishes elements while the predicate holds, completing at the first element that fails it.
    /// - Parameter predicate: A closure returning `true` to continue emitting.
    func prefix(while predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    guard predicate(v) else { raw.finish(); return }
                    if case .terminated = raw.yield(.success(v)) { return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Omits the first `count` elements, then republishes the remainder.
    /// - Parameter count: The number of elements to drop (default `1`).
    func dropFirst(_ count: Int = 1) -> Publisher<Output, Failure> {
        guard count > 0 else { return self }
        return _operator { raw, upstream in
            var dropped = 0
            for await result in upstream {
                switch result {
                case let .success(v):
                    if dropped < count { dropped += 1; continue }
                    if case .terminated = raw.yield(.success(v)) { return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Omits elements while the predicate holds, then republishes every element from the first failure onward.
    /// - Parameter predicate: A closure returning `true` to keep dropping.
    func drop(while predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var dropping = true
            for await result in upstream {
                switch result {
                case let .success(v):
                    if dropping {
                        if predicate(v) { continue }
                        dropping = false
                    }
                    if case .terminated = raw.yield(.success(v)) { return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }
}

// MARK: - output(at:) / output(in:)

public extension Publisher {
    /// Republishes only the element at the given zero-based index, then completes.
    /// - Parameter index: The position of the element to emit.
    func output(at index: Int) -> Publisher<Output, Failure> {
        dropFirst(index).first()
    }

    /// Republishes only the elements whose indices fall within the given range.
    /// - Parameter range: The range of zero-based positions to emit.
    func output(in range: Range<Int>) -> Publisher<Output, Failure> {
        guard !range.isEmpty else { return .empty() }
        return dropFirst(range.lowerBound).prefix(range.count)
    }
}

// MARK: - ignoreOutput

public extension Publisher {
    /// Discards all elements but preserves the completion or failure of the upstream.
    /// - Returns: A publisher that never emits a value and completes when the upstream does.
    func ignoreOutput() -> Publisher<Never, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Never, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Never, Failure>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success: continue
                        case let .failure(e):
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

public extension Publisher where Output: Equatable {
    /// Republishes elements, omitting any that are equal to the immediately preceding element.
    func removeDuplicates() -> Publisher<Output, Failure> {
        removeDuplicates(by: ==)
    }
}

public extension Publisher {
    /// Republishes elements, omitting any the predicate deems a duplicate of the immediately preceding element.
    /// - Parameter predicate: A closure returning `true` when two consecutive elements are considered equal.
    func removeDuplicates(
        by predicate: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var last: Output?
            for await result in upstream {
                switch result {
                case let .success(v):
                    if let l = last, predicate(l, v) { continue }
                    last = v
                    if case .terminated = raw.yield(.success(v)) { return }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }
}

// MARK: - contains / allSatisfy

public extension Publisher where Output: Equatable {
    /// Emits `true` and completes as soon as an element equal to `value` is seen, otherwise `false` on completion.
    /// - Parameter value: The element to search for.
    func contains(_ value: Output) -> Publisher<Bool, Failure> {
        contains(where: { $0 == value })
    }
}

public extension Publisher {
    /// Emits `true` and completes as soon as an element satisfies the predicate, otherwise `false` on completion.
    /// - Parameter predicate: A closure returning `true` for a matching element.
    func contains(where predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Bool, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                switch result {
                case let .success(v):
                    if predicate(v) {
                        _ = raw.yield(.success(true)); raw.finish(); return
                    }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            _ = raw.yield(.success(false))
            raw.finish()
        }
    }

    /// Emits a single `Bool` indicating whether every element satisfies the predicate, then completes.
    /// - Parameter predicate: A closure evaluated against each element; emits `false` early on the first failure.
    func allSatisfy(_ predicate: @escaping @Sendable (Output) -> Bool) -> Publisher<Bool, Failure> {
        contains(where: { !predicate($0) }).map { !$0 }
    }
}

// MARK: - min / max

public extension Publisher where Output: Comparable {
    /// Emits the minimum element once the upstream completes.
    func min() -> Publisher<Output, Failure> {
        _extremum(isLess: { @Sendable a, b in a < b })
    }

    /// Emits the maximum element once the upstream completes.
    func max() -> Publisher<Output, Failure> {
        _extremum(isLess: { @Sendable a, b in a > b })
    }
}

public extension Publisher {
    /// Emits the minimum element, using the provided ordering, once the upstream completes.
    /// - Parameter areInIncreasingOrder: A closure returning `true` if its first argument is ordered before its second.
    func min(
        by areInIncreasingOrder: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _extremum(isLess: areInIncreasingOrder)
    }

    /// Emits the maximum element, using the provided ordering, once the upstream completes.
    /// - Parameter areInIncreasingOrder: A closure returning `true` if its first argument is ordered before its second.
    func max(
        by areInIncreasingOrder: @escaping @Sendable (Output, Output) -> Bool
    ) -> Publisher<Output, Failure> {
        _extremum(isLess: { areInIncreasingOrder($1, $0) })
    }

    /// Emits `output` if the upstream completes without having produced any values.
    /// - Parameter output: The value to emit when the upstream is empty.
    func replaceEmpty(with output: Output) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            var emitted = false
            for await result in upstream {
                switch result {
                case let .success(v):
                    emitted = true
                    if case .terminated = raw.yield(.success(v)) { return }
                case let .failure(e):
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
                case let .success(v):
                    if let b = best { if isLess(v, b) { best = v } } else { best = v }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if let b = best { _ = raw.yield(.success(b)) }
            raw.finish()
        }
    }
}
