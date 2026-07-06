// SPDX-License-Identifier: Apache-2.0

public extension Publisher {
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

    func prepend(_ elements: Output...) -> Publisher<Output, Failure> {
        prepend(ContiguousArray(elements))
    }

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

    func append(_ elements: Output...) -> Publisher<Output, Failure> {
        append(ContiguousArray(elements))
    }

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

    func count() -> Publisher<Int, Failure> {
        reduce(0) { acc, _ in acc + 1 }
    }

    // Groups elements into arrays of at most `count`; the final array may be smaller.
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
