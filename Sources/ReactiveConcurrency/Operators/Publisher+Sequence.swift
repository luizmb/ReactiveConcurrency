// SPDX-License-Identifier: Apache-2.0

public extension Publisher {
    /// Emits the running accumulation, applying `next` to each element and emitting each intermediate result.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A closure folding the current accumulator and the next element into a new accumulator.
    /// - Returns: A publisher that emits every intermediate accumulator value.
    func scan<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case let .success(value):
                    acc = next(acc, value)
                    if case .terminated = raw.yield(.success(acc)) { return }
                case let .failure(error):
                    _ = raw.yield(.failure(error)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    /// Folds all elements into a single value, emitting only the final accumulator once the upstream completes.
    /// - Parameters:
    ///   - initial: The starting accumulator value.
    ///   - next: A closure folding the current accumulator and the next element into a new accumulator.
    /// - Returns: A publisher that emits the single final accumulator value.
    func reduce<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            var acc = initial
            for await result in upstream {
                switch result {
                case let .success(value):
                    acc = next(acc, value)
                case let .failure(error):
                    _ = raw.yield(.failure(error)); raw.finish(); return
                }
            }
            _ = raw.yield(.success(acc))
            raw.finish()
        }
    }

    /// Buffers all elements and emits them as a single array once the upstream completes.
    func collect() -> Publisher<[Output], Failure> {
        _operator { raw, upstream in
            var collected: [Output] = []
            for await result in upstream {
                switch result {
                case let .success(value):
                    collected.append(value)
                case let .failure(error):
                    _ = raw.yield(.failure(error)); raw.finish(); return
                }
            }
            _ = raw.yield(.success(collected))
            raw.finish()
        }
    }

    /// Emits the given elements before republishing the upstream's elements.
    /// - Parameter elements: The values to emit ahead of the upstream.
    func prepend(_ elements: Output...) -> Publisher<Output, Failure> {
        prepend(ContiguousArray(elements))
    }

    /// Emits the given sequence of elements before republishing the upstream's elements.
    /// - Parameter elements: The sequence of values to emit ahead of the upstream.
    func prepend<S: Sequence & Sendable>(
        _ elements: S
    ) -> Publisher<Output, Failure> where S.Element == Output {
        _operator { raw, upstream in
            for element in elements {
                if case .terminated = raw.yield(.success(element)) { return }
            }
            for await result in upstream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            raw.finish()
        }
    }

    /// Emits all of another publisher's elements before republishing the upstream's elements.
    /// - Parameter publisher: The publisher whose elements precede the upstream's.
    func prepend(_ publisher: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in publisher._stream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            for await result in upstream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            raw.finish()
        }
    }

    /// Emits the given elements after the upstream completes successfully.
    /// - Parameter elements: The values to emit once the upstream finishes.
    func append(_ elements: Output...) -> Publisher<Output, Failure> {
        append(ContiguousArray(elements))
    }

    /// Emits the given sequence of elements after the upstream completes successfully.
    /// - Parameter elements: The sequence of values to emit once the upstream finishes.
    func append<S: Sequence & Sendable>(
        _ elements: S
    ) -> Publisher<Output, Failure> where S.Element == Output {
        _operator { raw, upstream in
            for await result in upstream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            for element in elements {
                if case .terminated = raw.yield(.success(element)) { return }
            }
            raw.finish()
        }
    }

    /// Emits all of another publisher's elements after the upstream completes successfully.
    /// - Parameter publisher: The publisher whose elements follow the upstream's.
    func append(_ publisher: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in upstream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            for await result in publisher._stream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            raw.finish()
        }
    }

    /// Emits the total number of upstream elements once the upstream completes.
    func count() -> Publisher<Int, Failure> {
        reduce(0) { acc, _ in acc + 1 }
    }

    /// Groups elements into arrays of at most `count`; the final array may be smaller.
    /// - Parameter count: The maximum size of each emitted batch.
    /// - Returns: A publisher that emits arrays of buffered elements.
    func collect(_ count: Int) -> Publisher<[Output], Failure> {
        _operator { raw, upstream in
            var buffer: [Output] = []
            for await result in upstream {
                switch result {
                case let .success(v):
                    buffer.append(v)
                    if buffer.count >= count {
                        if case .terminated = raw.yield(.success(buffer)) { return }
                        buffer = []
                    }
                case let .failure(e):
                    _ = raw.yield(.failure(e)); raw.finish(); return
                }
            }
            if !buffer.isEmpty { _ = raw.yield(.success(buffer)) }
            raw.finish()
        }
    }
}
