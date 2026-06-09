import CoreFP

public final class PassthroughSubject<Output: Sendable, Failure: Error>: Sendable {
    private let _core: _SubjectCore<Output, Failure>

    public init() {
        _core = _SubjectCore()
    }

    // Synchronous — matches Combine's guarantee: values sent after sink() returns
    // are always delivered to that subscriber.
    public func send(_ value: Output) {
        _core.send(value)
    }

    public func send(completion: Subscribers.Completion<Failure>) {
        _core.complete(completion)
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        let core = _core
        return Publisher<Output, Failure>(DeferredStream {
            // subscribe() is synchronous — registration happens before this factory returns,
            // so any send() after sink() is guaranteed to reach this subscriber.
            let (id, stream) = core.subscribe()
            return AsyncStream<Result<Output, Failure>> { rawContinuation in
                let task = Task {
                    for await result in stream {
                        rawContinuation.yield(result)
                    }
                    rawContinuation.finish()
                }
                rawContinuation.onTermination = { _ in
                    task.cancel()
                    core.unsubscribe(id)
                }
            }
        })
    }
}
