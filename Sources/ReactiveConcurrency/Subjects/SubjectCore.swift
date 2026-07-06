// SPDX-License-Identifier: Apache-2.0

// Thread-safe hot-subject core. Uses Locked rather than an actor so that
// subscribe() and send() are synchronous — matching Combine's guarantee that
// values sent after sink() returns are always delivered to that subscriber.
final class SubjectCore<Output: Sendable, Failure: Error>: Sendable {
    private typealias Cont = AsyncStream<Result<Output, Failure>>.Continuation

    private struct _State {
        var subscribers: [Int: Cont] = [:]
        var completion: Subscribers.Completion<Failure>?
        var nextID: Int = 0
    }

    private let _state = Locked(_State())

    func subscribe() -> (id: Int, stream: AsyncStream<Result<Output, Failure>>) {
        _state.withLock { state in
            let (stream, cont) = AsyncStream<Result<Output, Failure>>.makeStream()
            if let completion = state.completion {
                switch completion {
                case .finished: cont.finish()
                case let .failure(e): cont.yield(Result.failure(e)); cont.finish()
                }
                // Return a fresh id even for already-completed subjects; unsubscribe is a no-op.
                let id = state.nextID; state.nextID += 1
                return (id, stream)
            }
            let id = state.nextID; state.nextID += 1
            state.subscribers[id] = cont
            return (id, stream)
        }
    }

    func unsubscribe(_ id: Int) {
        _state.withLock { state in
            state.subscribers[id]?.finish()
            state.subscribers.removeValue(forKey: id)
        }
    }

    func send(_ value: Output) {
        let droppedAfterCompletion = _state.withLock { state -> Bool in
            guard state.completion == nil else { return true }
            for cont in state.subscribers.values {
                cont.yield(Result.success(value))
            }
            return false
        }
        if droppedAfterCompletion {
            Diagnostics.warn("send(_:) called on a subject that has already completed — value dropped.")
        }
    }

    func complete(_ c: Subscribers.Completion<Failure>) {
        let ignoredAfterCompletion = _state.withLock { state -> Bool in
            guard state.completion == nil else { return true }
            state.completion = c
            switch c {
            case .finished:
                state.subscribers.values.forEach { $0.finish() }
            case let .failure(e):
                state.subscribers.values.forEach { $0.yield(Result.failure(e)); $0.finish() }
            }
            state.subscribers.removeAll()
            return false
        }
        if ignoredAfterCompletion {
            Diagnostics.warn("send(completion:) called on a subject that has already completed — ignored.")
        }
    }
}
