// SPDX-License-Identifier: Apache-2.0

/// Serialises the events of a stream that several tasks write to, so its terminal event is atomic.
///
/// Operators like `timeout` and the timed operators feed one downstream from two tasks: a producer
/// reading upstream and a consumer running the timed transform. Without a gate, the producer's
/// "forward the failure, then finish" can interleave with the consumer's "upstream ended, finish":
/// the consumer's `finish()` lands first and the failure is dropped, or a pending value is yielded
/// after the failure. Every write goes through the lock here, and once the stream has terminated
/// (finished, failed, or cancelled downstream) later writes are ignored.
final class TerminalGate<Element: Sendable>: Sendable {
    private let downstream: AsyncStream<Element>.Continuation
    private let terminated = Locked(false)

    init(_ downstream: AsyncStream<Element>.Continuation) {
        self.downstream = downstream
    }

    /// Yields `element` unless the stream has terminated.
    /// - Returns: `false` once the stream has terminated, so the writer can stop.
    @discardableResult
    func yield(_ element: Element) -> Bool {
        terminated.withLock { done in
            guard !done else { return false }
            if case .terminated = downstream.yield(element) {
                done = true
                return false
            }
            return true
        }
    }

    /// Terminates the stream, yielding `last` first when given. Only the first call has an effect.
    ///
    /// `finish()` runs outside the lock: it calls the stream's `onTermination` synchronously, and a
    /// handler that wrote back through the gate would otherwise deadlock on the non-recursive lock.
    func finish(with last: Element? = nil) {
        let first = terminated.withLock { done -> Bool in
            guard !done else { return false }
            done = true
            if let last { downstream.yield(last) }
            return true
        }
        if first { downstream.finish() }
    }
}
