extension Publisher {
    public func handleEvents(
        receiveSubscription: (@Sendable () -> Void)? = nil,
        receiveOutput: (@Sendable (Output) -> Void)? = nil,
        receiveCompletion: (@Sendable (Subscribers.Completion<Failure>) -> Void)? = nil,
        receiveCancel: (@Sendable () -> Void)? = nil
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            receiveSubscription?()
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success(let value):
                            receiveOutput?(value)
                            if case .terminated = raw.yield(Result.success(value)) {
                                receiveCancel?()
                                return
                            }
                        case .failure(let error):
                            receiveCompletion?(.failure(error))
                            _ = raw.yield(Result.failure(error))
                            raw.finish()
                            return
                        }
                    }
                    receiveCompletion?(.finished)
                    raw.finish()
                }
                raw.onTermination = { @Sendable reason in
                    task.cancel()
                    if case .cancelled = reason { receiveCancel?() }
                }
            }
        })
    }
}
