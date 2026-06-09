/// A clock whose `sleep` returns immediately.
/// Use in tests for operators that schedule work but where you don't want real wall-clock delays.
public struct ImmediateClock: Clock, Sendable {
    public typealias Duration = Swift.Duration

    public struct Instant: InstantProtocol, Sendable, Hashable {
        public var offset: Duration

        public init(offset: Duration = .zero) { self.offset = offset }

        public func advanced(by duration: Duration) -> Self { .init(offset: offset + duration) }
        public func duration(to other: Self) -> Duration { other.offset - offset }
        public static func < (lhs: Self, rhs: Self) -> Bool { lhs.offset < rhs.offset }
    }

    public var now: Instant { .init() }
    public var minimumResolution: Duration { .zero }

    public init() {}

    public func sleep(until deadline: Instant, tolerance: Duration?) async throws {
        try Task.checkCancellation()
    }
}
