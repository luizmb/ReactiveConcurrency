// SPDX-License-Identifier: Apache-2.0

// MARK: - share()

public extension Publisher {
    // Multicasts the upstream to multiple downstream subscribers.
    // The upstream subscription starts with the first subscriber and is torn down
    // when the last subscriber cancels. Subsequent re-subscriptions restart upstream.
    func share() -> Publisher<Output, Failure> {
        let shared = SharedState(upstream: self)
        return Publisher<Output, Failure>(DeferredStream {
            let (id, stream) = shared.subscribe()
            return AsyncStream<Result<Output, Failure>> { raw in
                let t = Task {
                    for await result in stream {
                        raw.yield(result)
                    }
                    raw.finish()
                }
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
        // Only bookkeeping under the lock: bump the refcount and register the subscriber
        // (SubjectCore.subscribe is pure dict insertion). The subscriber is registered before
        // the upstream pump is spawned, so no value can race ahead of it.
        let (isFirst, subscription): (Bool, (Int, AsyncStream<Result<Output, Failure>>)) =
            _state.withLock { state in
                state.refCount += 1
                return (state.refCount == 1, _core.subscribe())
            }
        guard isFirst else { return subscription }

        // Materialise the (arbitrary, possibly slow) upstream factory and spawn the pump
        // OUTSIDE the lock, so a blocking factory can't stall concurrent subscribe/unsubscribe.
        // Still synchronous within subscribe(), so values are not lost before this returns.
        let upstream = _upstream._stream.factory()
        let core = _core
        let task = Task {
            for await result in upstream {
                switch result {
                case let .success(v): core.send(v)
                case let .failure(e): core.complete(.failure(e)); return
                }
            }
            core.complete(.finished)
        }
        // Publish the task, unless everyone already unsubscribed in the meantime (refCount
        // back to 0) — in which case tear it down now rather than leak it.
        let cancelNow: Bool = _state.withLock { state in
            guard state.refCount > 0 else { return true }
            state.upstreamTask = task
            return false
        }
        if cancelNow { task.cancel() }
        return subscription
    }

    func unsubscribe(_ id: Int) {
        // Capture the task to cancel under the lock; run cancel() after release. Task.cancel()
        // synchronously runs AsyncStream termination handlers on the calling thread, and those
        // handlers take locks — keeping it out of the critical section removes the stall/deadlock risk.
        let toCancel: Task<Void, Never>? = _state.withLock { state in
            _core.unsubscribe(id)
            state.refCount -= 1
            guard state.refCount == 0 else { return nil }
            let task = state.upstreamTask
            state.upstreamTask = nil
            return task
        }
        toCancel?.cancel()
    }
}

// MARK: - autoconnect helper

// Holds the AnyCancellable from connect() as a shared reference so it outlives
// any individual subscriber's onTermination closure.
private final class AutoconnectState: Sendable {
    private struct _State {
        var started = false
        var connection: AnyCancellable?
    }

    private let _state = Locked(_State())

    func connectOnce(_ connect: @Sendable () -> AnyCancellable) {
        // Reserve the single connection under the lock via a flag, then run connect()
        // (factory + Task spawn) outside it. The `started` flag guarantees exactly-once
        // without holding the lock across the arbitrary connect work.
        let shouldConnect: Bool = _state.withLock { state in
            guard !state.started else { return false }
            state.started = true
            return true
        }
        guard shouldConnect else { return }
        let conn = connect()
        _state.withLock { $0.connection = conn }
    }
}

// MARK: - makeConnectable() / ConnectablePublisher

public extension Publisher {
    /// A connectable publisher that fans the upstream out through an internal `PassthroughSubject`.
    func makeConnectable() -> ConnectablePublisher<Output, Failure> {
        ConnectablePublisher(upstream: self, subject: PassthroughSubject<Output, Failure>().eraseToAnySubject())
    }

    /// Multicasts the upstream through the given subject. The subject's semantics drive fan-out —
    /// e.g. a `CurrentValueSubject` replays the latest value to late subscribers.
    func multicast<S: Subject>(
        subject: S
    ) -> ConnectablePublisher<Output, Failure> where S.Output == Output, S.Failure == Failure {
        ConnectablePublisher(upstream: self, subject: subject.eraseToAnySubject())
    }

    /// Multicasts the upstream through a subject created by `createSubject`.
    func multicast<S: Subject>(
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
                case let .success(v): subject.send(v)
                case let .failure(e): subject.send(completion: .failure(e)); return
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
                let t = Task {
                    for await result in downstream {
                        raw.yield(result)
                    }
                    raw.finish()
                }
                raw.onTermination = { [state] _ in
                    t.cancel()
                    _ = state // keep connection alive as long as any subscriber is alive
                }
            }
        })
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        _subject.eraseToPublisher()
    }
}
