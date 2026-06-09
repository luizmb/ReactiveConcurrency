import Synchronization

public final class CurrentValueSubject<Output: Sendable, Failure: Error>: Sendable {
    private let _core: _CurrentValueCore<Output, Failure>

    public init(_ value: Output) {
        _core = _CurrentValueCore(value)
    }

    public var value: Output { _core.currentValue }

    public func send(_ value: Output) {
        _core.send(value)
    }

    public func send(completion: Subscribers.Completion<Failure>) {
        _core.complete(completion)
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        let core = _core
        return Publisher<Output, Failure>(DeferredStream {
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

final class _CurrentValueCore<Output: Sendable, Failure: Error>: Sendable {
    private typealias Cont = AsyncStream<Result<Output, Failure>>.Continuation

    private struct _State {
        var current: Output
        var subscribers: [Int: Cont] = [:]
        var completion: Subscribers.Completion<Failure>? = nil
        var nextID: Int = 0
    }
    private let _state: Mutex<_State>

    init(_ initial: Output) {
        _state = Mutex(_State(current: initial))
    }

    var currentValue: Output { _state.withLock { $0.current } }

    func subscribe() -> (id: Int, stream: AsyncStream<Result<Output, Failure>>) {
        _state.withLock { state in
            let (stream, cont) = AsyncStream<Result<Output, Failure>>.makeStream()
            cont.yield(Result.success(state.current))  // replay current value synchronously
            if let completion = state.completion {
                switch completion {
                case .finished: cont.finish()
                case .failure(let e): cont.yield(Result.failure(e)); cont.finish()
                }
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
        _state.withLock { state in
            guard state.completion == nil else { return }
            state.current = value
            for cont in state.subscribers.values { cont.yield(Result.success(value)) }
        }
    }

    func complete(_ c: Subscribers.Completion<Failure>) {
        _state.withLock { state in
            guard state.completion == nil else { return }
            state.completion = c
            switch c {
            case .finished:
                state.subscribers.values.forEach { $0.finish() }
            case .failure(let e):
                state.subscribers.values.forEach { $0.yield(Result.failure(e)); $0.finish() }
            }
            state.subscribers.removeAll()
        }
    }
}
