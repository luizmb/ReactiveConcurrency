import CoreFP

extension Publisher {
    @discardableResult
    public func sink(
        receiveCompletion: @escaping @Sendable (Subscribers.Completion<Failure>) -> Void,
        receiveValue: @escaping @Sendable (Output) -> Void
    ) -> AnyCancellable {
        // _StreamBox calls makeAsyncIterator() synchronously — for subject-backed publishers
        // this registers the subscription before sink() returns, so values sent after
        // sink() are guaranteed to reach this subscriber.
        let box = _StreamBox<Result<Output, Failure>>(_stream)
        let task = Task {
            while let result = await box.next() {
                switch result {
                case .success(let value):
                    receiveValue(value)
                case .failure(let error):
                    receiveCompletion(.failure(error))
                    return
                }
            }
            receiveCompletion(.finished)
        }
        return AnyCancellable { task.cancel() }
    }
}

extension Publisher where Failure == Never {
    @discardableResult
    public func sink(
        receiveValue: @escaping @Sendable (Output) -> Void
    ) -> AnyCancellable {
        let box = _StreamBox<Result<Output, Never>>(_stream)
        let task = Task {
            while let result = await box.next() {
                if case .success(let value) = result { receiveValue(value) }
            }
        }
        return AnyCancellable { task.cancel() }
    }
}
