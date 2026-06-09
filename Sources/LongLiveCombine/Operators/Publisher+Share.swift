import Foundation
import CoreFP

// MARK: - share()

extension Publisher {
    // Multicasts the upstream to multiple downstream subscribers.
    // The upstream subscription starts with the first subscriber and is torn down
    // when the last subscriber cancels. Subsequent re-subscriptions restart upstream.
    public func share() -> Publisher<Output, Failure> {
        let shared = _SharedState(upstream: self)
        return Publisher<Output, Failure>(DeferredStream {
            let (id, stream) = shared.subscribe()
            return AsyncStream<Result<Output, Failure>> { raw in
                let t = Task { for await result in stream { raw.yield(result) }; raw.finish() }
                raw.onTermination = { _ in t.cancel(); shared.unsubscribe(id) }
            }
        })
    }
}

// @unchecked Sendable: all mutations protected by _lock.
private final class _SharedState<Output: Sendable, Failure: Error>: @unchecked Sendable {
    private let _upstream: Publisher<Output, Failure>
    private let _core: _SubjectCore<Output, Failure>
    private let _lock = NSLock()
    private var _refCount = 0
    private var _upstreamBox: _StreamBox<Result<Output, Failure>>?
    private var _upstreamTask: Task<Void, Never>?

    init(upstream: Publisher<Output, Failure>) {
        _upstream = upstream
        _core = _SubjectCore()
    }

    func subscribe() -> (UUID, AsyncStream<Result<Output, Failure>>) {
        _lock.withLock {
            _refCount += 1
            if _refCount == 1 {
                // Pre-subscribe upstream synchronously so values sent after the first
                // downstream sink() call are not lost while the Task starts.
                let box = _StreamBox<Result<Output, Failure>>(_upstream._stream)
                _upstreamBox = box
                let core = _core
                _upstreamTask = Task {
                    while let result = await box.next() {
                        switch result {
                        case .success(let v): core.send(v)
                        case .failure(let e): core.complete(.failure(e)); return
                        }
                    }
                    core.complete(.finished)
                }
            }
            return _core.subscribe()
        }
    }

    func unsubscribe(_ id: UUID) {
        _lock.withLock {
            _core.unsubscribe(id)
            _refCount -= 1
            if _refCount == 0 {
                _upstreamTask?.cancel()
                _upstreamTask = nil
                _upstreamBox = nil
            }
        }
    }
}

// MARK: - makeConnectable() / ConnectablePublisher

extension Publisher {
    public func makeConnectable() -> ConnectablePublisher<Output, Failure> {
        ConnectablePublisher(upstream: self)
    }
}

public struct ConnectablePublisher<Output: Sendable, Failure: Error>: Sendable {
    private let _upstream: Publisher<Output, Failure>
    private let _core: _SubjectCore<Output, Failure>

    init(upstream: Publisher<Output, Failure>) {
        _upstream = upstream
        _core = _SubjectCore()
    }

    // Starts the upstream and fans values to all current and future subscribers.
    // Returns a cancellable that disconnects when cancelled.
    @discardableResult
    public func connect() -> AnyCancellable {
        let core = _core
        let box = _StreamBox<Result<Output, Failure>>(_upstream._stream)
        let task = Task {
            while let result = await box.next() {
                switch result {
                case .success(let v): core.send(v)
                case .failure(let e): core.complete(.failure(e)); return
                }
            }
            core.complete(.finished)
        }
        return AnyCancellable { task.cancel() }
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        let core = _core
        return Publisher<Output, Failure>(DeferredStream {
            let (id, stream) = core.subscribe()
            return AsyncStream<Result<Output, Failure>> { raw in
                let t = Task { for await result in stream { raw.yield(result) }; raw.finish() }
                raw.onTermination = { _ in t.cancel(); core.unsubscribe(id) }
            }
        })
    }
}
