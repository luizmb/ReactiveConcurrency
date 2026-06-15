public protocol Cancellable: Sendable {
    func cancel()
}

public final class AnyCancellable: Cancellable, Hashable {
    private let _cancel: @Sendable () -> Void
    private let _executed = Locked(false)

    public init(_ cancel: @escaping @Sendable () -> Void) {
        _cancel = cancel
    }

    public func cancel() {
        let shouldRun = _executed.withLock { executed in
            guard !executed else { return false }
            executed = true
            return true
        }
        if shouldRun { _cancel() }
    }

    deinit { cancel() }

    public static func == (lhs: AnyCancellable, rhs: AnyCancellable) -> Bool { lhs === rhs }
    public func hash(into hasher: inout Hasher) { hasher.combine(ObjectIdentifier(self)) }
}

extension AnyCancellable {
    public func store(in collection: inout some RangeReplaceableCollection<AnyCancellable>) {
        collection.append(self)
    }

    public func store(in set: inout Set<AnyCancellable>) {
        set.insert(self)
    }
}
