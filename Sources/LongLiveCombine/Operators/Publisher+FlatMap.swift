import CoreFP

extension Publisher {
    // All inner publishers run concurrently; outputs merged in arrival order.
    // First failure (upstream or any inner) immediately seals the downstream.
    public func flatMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> Publisher<T, Failure> {
        let selfStream = _stream
        return Publisher<T, Failure>(DeferredStream {
            let upstream = _StreamBox<Result<Output, Failure>>(selfStream)
            return AsyncStream<Result<T, Failure>> { raw in
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        outer: while let result = await upstream.next() {
                            switch result {
                            case .success(let value):
                                let inner = transform(value)
                                group.addTask {
                                    for await innerResult in inner._stream {
                                        let yr = raw.yield(innerResult)
                                        if case .terminated = yr { return }
                                        if case .failure = innerResult { raw.finish(); return }
                                    }
                                }
                            case .failure(let error):
                                _ = raw.yield(Result.failure(error))
                                raw.finish()
                                break outer
                            }
                        }
                        for await _ in group {}
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    // Cancels the current inner publisher whenever a new upstream value arrives.
    // Only the most recent inner publisher's values reach downstream.
    public func switchToLatest<T: Sendable>() -> Publisher<T, Failure>
        where Output == Publisher<T, Failure> {
        _operator { raw, upstream in
            var innerTask: Task<Void, Never>?
            while let result = await upstream.next() {
                switch result {
                case .success(let inner):
                    innerTask?.cancel()
                    innerTask = Task {
                        for await innerResult in inner._stream {
                            guard !Task.isCancelled else { return }
                            let yr = raw.yield(innerResult)
                            if case .terminated = yr { return }
                            if case .failure = innerResult { raw.finish(); return }
                        }
                    }
                case .failure(let error):
                    innerTask?.cancel()
                    _ = raw.yield(Result.failure(error))
                    raw.finish()
                    return
                }
            }
            innerTask?.cancel()
            raw.finish()
        }
    }
}
