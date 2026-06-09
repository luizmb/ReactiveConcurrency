public protocol Cancellable: Sendable {
    func cancel()
}

public final class AnyCancellable: Cancellable, Hashable {
    private let _cancel: @Sendable () -> Void

    public init(_ cancel: @escaping @Sendable () -> Void) {
        _cancel = cancel
    }

    public func cancel() { _cancel() }
    deinit { _cancel() }

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
