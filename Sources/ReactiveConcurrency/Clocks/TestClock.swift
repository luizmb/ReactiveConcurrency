import Synchronization

/// A clock whose time only advances when you call `advance(by:)`.
/// Use in tests to drive timing operators (`debounce`, `delay`, `timer`, etc.) deterministically.
public final class TestClock: Clock, @unchecked Sendable {
    public typealias Duration = Swift.Duration

    public struct Instant: InstantProtocol, Sendable, Hashable {
        public var offset: Duration

        public init(offset: Duration = .zero) { self.offset = offset }

        public func advanced(by duration: Duration) -> Self { .init(offset: offset + duration) }
        public func duration(to other: Self) -> Duration { other.offset - offset }
        public static func < (lhs: Self, rhs: Self) -> Bool { lhs.offset < rhs.offset }
    }

    private struct _State {
        var now: Instant = .init()
        var sleepers: [(deadline: Instant, continuation: CheckedContinuation<Void, Error>)] = []
    }

    private let _state = Mutex(_State())

    public var now: Instant { _state.withLock { $0.now } }
    public var minimumResolution: Duration { .nanoseconds(1) }

    public init() {}

    public func sleep(until deadline: Instant, tolerance: Duration?) async throws {
        try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
            _state.withLock { state in
                if state.now >= deadline {
                    c.resume()
                } else {
                    state.sleepers.append((deadline: deadline, continuation: c))
                    state.sleepers.sort { $0.deadline < $1.deadline }
                }
            }
        }
        // Propagate cancellation even if the continuation was resumed normally.
        try Task.checkCancellation()
    }

    /// Advance virtual time by `duration`, waking all tasks sleeping past the new `now`.
    /// Yields between each woken task so they can run and register new sleeps before
    /// the next `advance` call.
    public func advance(by duration: Duration) async {
        let toResume = _state.withLock { state -> [CheckedContinuation<Void, Error>] in
            state.now = state.now.advanced(by: duration)
            let ready = state.sleepers.filter { $0.deadline <= state.now }.map(\.continuation)
            state.sleepers.removeAll { $0.deadline <= state.now }
            return ready
        }
        for continuation in toResume {
            continuation.resume()
            await Task.yield()
        }
    }
}
