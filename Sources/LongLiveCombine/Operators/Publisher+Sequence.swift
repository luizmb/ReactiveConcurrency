extension Publisher {
    public func scan<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            var acc = initial
            while let result = await upstream.next() {
                switch result {
                case .success(let value):
                    acc = next(acc, value)
                    if case .terminated = raw.yield(Result.success(acc)) { return }
                case .failure(let error):
                    _ = raw.yield(Result.failure(error)); raw.finish(); return
                }
            }
            raw.finish()
        }
    }

    public func reduce<T: Sendable>(
        _ initial: T,
        _ next: @escaping @Sendable (T, Output) -> T
    ) -> Publisher<T, Failure> {
        _operator { raw, upstream in
            var acc = initial
            while let result = await upstream.next() {
                switch result {
                case .success(let value):
                    acc = next(acc, value)
                case .failure(let error):
                    _ = raw.yield(Result.failure(error)); raw.finish(); return
                }
            }
            _ = raw.yield(Result.success(acc))
            raw.finish()
        }
    }

    public func collect() -> Publisher<[Output], Failure> {
        _operator { raw, upstream in
            var collected: [Output] = []
            while let result = await upstream.next() {
                switch result {
                case .success(let value):
                    collected.append(value)
                case .failure(let error):
                    _ = raw.yield(Result.failure(error)); raw.finish(); return
                }
            }
            _ = raw.yield(Result.success(collected))
            raw.finish()
        }
    }

    public func prepend(_ elements: Output...) -> Publisher<Output, Failure> {
        prepend(ContiguousArray(elements))
    }

    public func prepend<S: Sequence & Sendable>(
        _ elements: S
    ) -> Publisher<Output, Failure> where S.Element == Output {
        _operator { raw, upstream in
            for element in elements {
                if case .terminated = raw.yield(Result.success(element)) { return }
            }
            while let result = await upstream.next() {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            raw.finish()
        }
    }

    public func prepend(_ publisher: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            for await result in publisher._stream {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            while let result = await upstream.next() {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            raw.finish()
        }
    }

    public func append(_ elements: Output...) -> Publisher<Output, Failure> {
        append(ContiguousArray(elements))
    }

    public func append<S: Sequence & Sendable>(
        _ elements: S
    ) -> Publisher<Output, Failure> where S.Element == Output {
        _operator { raw, upstream in
            while let result = await upstream.next() {
                if case .terminated = raw.yield(result) { return }
                if case .failure = result { raw.finish(); return }
            }
            for element in elements {
                if case .terminated = raw.yield(Result.success(element)) { return }
            }
            raw.finish()
        }
    }

    public func append(_ publisher: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            while let result = await upstream.next() {
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
}
