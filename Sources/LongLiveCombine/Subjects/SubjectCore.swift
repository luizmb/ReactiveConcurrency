import Foundation

// Thread-safe hot-subject core. Uses NSLock rather than an actor so that
// subscribe() and send() are synchronous — matching Combine's guarantee that
// values sent after sink() returns are always delivered to that subscriber.
// @unchecked Sendable: all mutations are protected by _lock.
final class _SubjectCore<Output: Sendable, Failure: Error>: @unchecked Sendable {
    private typealias Cont = AsyncStream<Result<Output, Failure>>.Continuation

    private let _lock = NSLock()
    private var _subscribers: [UUID: Cont] = [:]
    private var _completion: Subscribers.Completion<Failure>?

    func subscribe() -> (id: UUID, stream: AsyncStream<Result<Output, Failure>>) {
        _lock.withLock {
            if let completion = _completion {
                let (stream, cont) = AsyncStream<Result<Output, Failure>>.makeStream()
                switch completion {
                case .finished:
                    cont.finish()
                case .failure(let error):
                    cont.yield(Result.failure(error))
                    cont.finish()
                }
                return (UUID(), stream)
            }

            let id = UUID()
            let (stream, cont) = AsyncStream<Result<Output, Failure>>.makeStream()
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
