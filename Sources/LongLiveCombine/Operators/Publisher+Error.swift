import CoreFP

extension Publisher {
    // Replaces failure with a recovery publisher; downstream becomes infallible.
    public func `catch`(
        _ handler: @escaping @Sendable (Failure) -> Publisher<Output, Never>
    ) -> Publisher<Output, Never> {
        let selfStream = _stream
        return Publisher<Output, Never>(DeferredStream {
            let upstream = _StreamBox<Result<Output, Failure>>(selfStream)
            return AsyncStream<Result<Output, Never>> { raw in
                let task = Task {
                    while let result = await upstream.next() {
                        switch result {
                        case .success(let value):
                            if case .terminated = raw.yield(Result.success(value)) { return }
                        case .failure(let error):
                            for await r in handler(error)._stream {
                                if case .terminated = raw.yield(r) { return }
                            }
                            raw.finish()
                            return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    // Replaces failure with a recovery publisher of the same failure type.
    public func `catch`(
        _ handler: @escaping @Sendable (Failure) -> Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        _operator { raw, upstream in
            while let result = await upstream.next() {
                switch result {
                case .success(let value):
                    if case .terminated = raw.yield(Result.success(value)) { return }
                case .failure(let error):
                    for await r in handler(error)._stream {
                        if case .terminated = raw.yield(r) { return }
                        if case .failure = r { raw.finish(); return }
                    }
                    raw.finish()
                    return
                }
            }
            raw.finish()
        }
    }

    public func replaceError(with output: Output) -> Publisher<Output, Never> {
        let selfStream = _stream
        return Publisher<Output, Never>(DeferredStream {
            let upstream = _StreamBox<Result<Output, Failure>>(selfStream)
            return AsyncStream<Result<Output, Never>> { raw in
                let task = Task {
                    while let result = await upstream.next() {
                        switch result {
                        case .success(let value):
                            if case .terminated = raw.yield(Result.success(value)) { return }
                        case .failure:
                            _ = raw.yield(Result.success(output))
                            raw.finish()
                            return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    public func retry(_ times: Int) -> Publisher<Output, Failure> {
        guard times > 0 else { return self }
        return Publisher<Output, Failure>(DeferredStream {
            AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    var remaining = times
                    while true {
                        var shouldRetry = false
                        let box = _StreamBox<Result<Output, Failure>>(self._stream)
                        loop: while let result = await box.next() {
                            switch result {
                            case .success(let v):
                                if case .terminated = raw.yield(Result.success(v)) { return }
                            case .failure(let e):
                                if remaining > 0 {
                                    remaining -= 1
                                    shouldRetry = true
                                } else {
                                    _ = raw.yield(Result.failure(e))
                                    raw.finish()
                                    return
                                }
                                break loop
                            }
                        }
                        if !shouldRetry { break }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
