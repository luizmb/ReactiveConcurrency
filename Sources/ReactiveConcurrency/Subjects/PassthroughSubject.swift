// SPDX-License-Identifier: Apache-2.0
///
/// Values sent while no subscriber is attached are lost. New subscribers only receive values sent
/// after they subscribe. Delivery is asynchronous, so reentrancy anomalies cannot occur.

/// A hot subject that broadcasts each sent value to current subscribers, with no replay.
public final class PassthroughSubject<Output: Sendable, Failure: Error>: Subject {
    private let _core: SubjectCore<Output, Failure>

    /// Creates an empty subject with no subscribers.
    public init() {
        _core = SubjectCore()
    }

    // Synchronous — matches Combine's guarantee: values sent after sink() returns
    // are always delivered to that subscriber.

    /// Broadcasts a value to all current subscribers; dropped if none are attached.
    public func send(_ value: Output) {
        _core.send(value)
    }

    /// Terminates the subject with a completion; subsequent sends are ignored.
    public func send(completion: Subscribers.Completion<Failure>) {
        _core.complete(completion)
    }

    /// Exposes this subject as a read-only `Publisher`.
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
