import Foundation
import CoreFP

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

// @unchecked Sendable: all mutations are protected by _lock.
final class _CurrentValueCore<Output: Sendable, Failure: Error>: @unchecked Sendable {
    private typealias Cont = AsyncStream<Result<Output, Failure>>.Continuation

    private let _lock = NSLock()
    private var _current: Output
    private var _subscribers: [UUID: Cont] = [:]
    private var _completion: Subscribers.Completion<Failure>?

    init(_ initial: Output) {
        _current = initial
    }

    var currentValue: Output { _lock.withLock { _current } }

    func subscribe() -> (id: UUID, stream: AsyncStream<Result<Output, Failure>>) {
        _lock.withLock {
            let (stream, cont) = AsyncStream<Result<Output, Failure>>.makeStream()
            cont.yield(Result.success(_current))  // replay current value synchronously

            if let completion = _completion {
                switch completion {
                case .finished: cont.finish()
                case .failure(let e): cont.yield(Result.failure(e)); cont.finish()
                }
                return (UUID(), stream)
            }

            let id = UUID()
            _subscribers[id] = cont
            return (id, stream)
        }
    }

    func unsubscribe(_ id: UUID) {
        _lock.withLock {
            _subscribers[id]?.finish()
            _subscribers.removeValue(forKey: id)
        }
    }

    func send(_ value: Output) {
        _lock.withLock {
            guard _completion == nil else { return }
            _current = value
            for cont in _subscribers.values { cont.yield(Result.success(value)) }
        }
    }

    func complete(_ c: Subscribers.Completion<Failure>) {
        _lock.withLock {
            guard _completion == nil else { return }
            _completion = c
            switch c {
            case .finished:
                _subscribers.values.forEach { $0.finish() }
            case .failure(let error):
                _subscribers.values.forEach {
                    $0.yield(Result.failure(error))
                    $0.finish()
                }
            }
            _subscribers.removeAll()
        }
    }
}
