// SPDX-License-Identifier: Apache-2.0

/// Something whose activity can be cancelled, releasing its resources.
public protocol Cancellable: Sendable {
    /// Cancels the underlying activity.
    func cancel()
}

/// A type-erased cancellable that runs its cancel closure once, and automatically on `deinit`.
///
/// Dropping the instance (letting it deallocate) cancels; keep it alive to keep the work running.
public final class AnyCancellable: Cancellable, Hashable {
    private let _cancel: @Sendable () -> Void
    private let _executed = Locked(false)

    /// Wraps a cancel closure, invoked at most once by `cancel()` or `deinit`.
    public init(_ cancel: @escaping @Sendable () -> Void) {
        _cancel = cancel
    }

    /// Runs the cancel closure exactly once; further calls are no-ops.
    public func cancel() {
        let shouldRun = _executed.withLock { executed in
            guard !executed else { return false }
            executed = true
            return true
        }
        if shouldRun { _cancel() }
    }

    deinit { cancel() }

    /// Identity equality — two instances are equal only if they are the same object.
    public static func == (lhs: AnyCancellable, rhs: AnyCancellable) -> Bool { lhs === rhs }
    /// Hashes by object identity.
    public func hash(into hasher: inout Hasher) { hasher.combine(ObjectIdentifier(self)) }
}

public extension AnyCancellable {
    /// Stores this cancellable in a collection so it stays alive alongside it.
    func store(in collection: inout some RangeReplaceableCollection<AnyCancellable>) {
        collection.append(self)
    }

    /// Stores this cancellable in a set so it stays alive alongside it.
    func store(in set: inout Set<AnyCancellable>) {
        set.insert(self)
    }
}
