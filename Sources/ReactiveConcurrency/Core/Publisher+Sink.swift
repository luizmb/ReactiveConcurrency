extension Publisher {
    public func sink(
        receiveCompletion: @escaping @Sendable (Subscribers.Completion<Failure>) -> Void,
        receiveValue: @escaping @Sendable (Output) -> Void
    ) -> AnyCancellable {
        // factory() is called synchronously — for subject-backed publishers this registers
        // the subscription before sink() returns, guaranteeing delivery of all subsequent sends.
        let stream = _stream.factory()
        let task = Task {
            for await result in stream {
                // AsyncStream.AsyncIterator.next() returns buffered values without checking
                // the cancellation flag — it only checks when it would need to suspend.
                // Guard here ensures a pre-cancelled Task (e.g. dropped AnyCancellable) never
                // delivers buffered values, matching cooperative-cancellation expectations.
                guard !Task.isCancelled else { break }
                switch result {
                case .success(let value):
                    receiveValue(value)
                case .failure(let error):
                    receiveCompletion(.failure(error))
                    return
                }
            }
            if !Task.isCancelled {
                receiveCompletion(.finished)
            }
        }
        return AnyCancellable { task.cancel() }
    }
}

extension Publisher where Failure == Never {
    public func sink(receiveValue: @escaping @Sendable (Output) -> Void) -> AnyCancellable {
        let stream = _stream.factory()
        let task = Task {
            for await result in stream {
                guard !Task.isCancelled else { break }
                if case .success(let value) = result { receiveValue(value) }
            }
        }
        return AnyCancellable { task.cancel() }
    }
}
