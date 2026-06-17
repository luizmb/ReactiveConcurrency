// MARK: - share()

extension Publisher {
    // Multicasts the upstream to multiple downstream subscribers.
    // The upstream subscription starts with the first subscriber and is torn down
    // when the last subscriber cancels. Subsequent re-subscriptions restart upstream.
    public func share() -> Publisher<Output, Failure> {
        let shared = SharedState(upstream: self)
        return Publisher<Output, Failure>(DeferredStream {
            let (id, stream) = shared.subscribe()
            return AsyncStream<Result<Output, Failure>> { raw in
                let t = Task { for await result in stream { raw.yield(result) }; raw.finish() }
                raw.onTermination = { _ in t.cancel(); shared.unsubscribe(id) }
            }
        })
    }
}

private final class SharedState<Output: Sendable, Failure: Error>: Sendable {
    private let _upstream: Publisher<Output, Failure>
    private let _core: SubjectCore<Output, Failure>

    private struct _State {
        var refCount: Int = 0
        var upstreamTask: Task<Void, Never>?
    }
    private let _state = Locked(_State())

    init(upstream: Publisher<Output, Failure>) {
        _upstream = upstream
        _core = SubjectCore()
    }

    func subscribe() -> (Int, AsyncStream<Result<Output, Failure>>) {
        _state.withLock { state in
            state.refCount += 1
            if state.refCount == 1 {
                // Pre-subscribe upstream synchronously so values sent after the first
                // downstream sink() call are not lost while the Task starts.
                let upstream = _upstream._stream.factory()
                let core = _core
                state.upstreamTask = Task {
                    for await result in upstream {
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

    func unsubscribe(_ id: Int) {
        _state.withLock { state in
            _core.unsubscribe(id)
            state.refCount -= 1
            if state.refCount == 0 {
                state.upstreamTask?.cancel()
                state.upstreamTask = nil
            }
        }
    }
}

// MARK: - autoconnect helper

// Holds the AnyCancellable from connect() as a shared reference so it outlives
// any individual subscriber's onTermination closure.
private final class AutoconnectState: Sendable {
    private let _connection: Locked<AnyCancellable?> = Locked(nil)

    func connectOnce(_ connect: @Sendable () -> AnyCancellable) {
        _connection.withLock { conn in
            guard conn == nil else { return }
            conn = connect()
        }
    }
}

// MARK: - makeConnectable() / ConnectablePublisher

extension Publisher {
    /// A connectable publisher that fans the upstream out through an internal `PassthroughSubject`.
    public func makeConnectable() -> ConnectablePublisher<Output, Failure> {
        ConnectablePublisher(upstream: self, subject: PassthroughSubject<Output, Failure>().eraseToAnySubject())
    }

    /// Multicasts the upstream through the given subject. The subject's semantics drive fan-out —
    /// e.g. a `CurrentValueSubject` replays the latest value to late subscribers.
    public func multicast<S: Subject>(
        subject: S
    ) -> ConnectablePublisher<Output, Failure> where S.Output == Output, S.Failure == Failure {
        ConnectablePublisher(upstream: self, subject: subject.eraseToAnySubject())
    }

    /// Multicasts the upstream through a subject created by `createSubject`.
    public func multicast<S: Subject>(
        _ createSubject: @Sendable () -> S
    ) -> ConnectablePublisher<Output, Failure> where S.Output == Output, S.Failure == Failure {
        ConnectablePublisher(upstream: self, subject: createSubject().eraseToAnySubject())
    }
}

public struct ConnectablePublisher<Output: Sendable, Failure: Error>: Sendable {
    private let _upstream: Publisher<Output, Failure>
    private let _subject: AnySubject<Output, Failure>

    init(upstream: Publisher<Output, Failure>, subject: AnySubject<Output, Failure>) {
        _upstream = upstream
        _subject = subject
    }

    // Starts the upstream and fans values into the subject (and thus to all subscribers).
    // Returns a cancellable that disconnects when cancelled.
    @discardableResult
    public func connect() -> AnyCancellable {
        let upstream = _upstream._stream.factory()
        let subject = _subject
        let task = Task {
            for await result in upstream {
                switch result {
                case .success(let v): subject.send(v)
                case .failure(let e): subject.send(completion: .failure(e)); return
                }
            }
            subject.send(completion: .finished)
        }
        return AnyCancellable { task.cancel() }
    }

    // Connects automatically on first subscription; stays connected until the source completes.
    // Unlike share(), subscribers cancelling does not disconnect the upstream.
    public func autoconnect() -> Publisher<Output, Failure> {
        let state = AutoconnectState()
        let connectable = self
        let subject = _subject
        return Publisher<Output, Failure>(DeferredStream {
            state.connectOnce { connectable.connect() }
            let downstream = subject.eraseToPublisher()._stream.factory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let t = Task { for await result in downstream { raw.yield(result) }; raw.finish() }
                raw.onTermination = { [state] _ in
                    t.cancel()
                    _ = state  // keep connection alive as long as any subscriber is alive
                }
            }
        })
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        _subject.eraseToPublisher()
    }
}
