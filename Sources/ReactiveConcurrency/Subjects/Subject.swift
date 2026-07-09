// SPDX-License-Identifier: Apache-2.0
/// A hot, imperatively-driven publisher that callers push values and completion into.
///
/// Subjects are hot: values sent while no subscriber is attached are lost. Delivery is
/// asynchronous, so a subscriber can never re-enter `send` mid-delivery (no reentrancy anomalies).
public protocol Subject<Output, Failure>: AnyObject, Sendable {
    /// The type of values this subject emits.
    associatedtype Output: Sendable
    /// The type of failure this subject can terminate with.
    associatedtype Failure: Error

    /// Pushes a value to all current subscribers; dropped if none are attached.
    func send(_ value: Output)
    /// Terminates the subject with a completion; subsequent sends are ignored.
    func send(completion: Subscribers.Completion<Failure>)
    /// Exposes this subject as a read-only `Publisher`.
    func eraseToPublisher() -> Publisher<Output, Failure>
}

public extension Subject {
    /// Wraps this subject in a type-erased `AnySubject` value.
    func eraseToAnySubject() -> AnySubject<Output, Failure> {
        AnySubject(self)
    }
}

/// A type-erased, `Sendable` wrapper over any `Subject` with the same `Output` and `Failure`.
public struct AnySubject<Output: Sendable, Failure: Error>: Sendable {
    private let _send: @Sendable (Output) -> Void
    private let _complete: @Sendable (Subscribers.Completion<Failure>) -> Void
    private let _erase: @Sendable () -> Publisher<Output, Failure>

    /// Wraps a concrete subject, capturing its operations behind `Sendable` closures.
    public init<S: Subject>(_ subject: S) where S.Output == Output, S.Failure == Failure {
        _send = { subject.send($0) }
        _complete = { subject.send(completion: $0) }
        _erase = { subject.eraseToPublisher() }
    }

    /// Pushes a value to the wrapped subject.
    public func send(_ value: Output) { _send(value) }
    /// Terminates the wrapped subject with a completion.
    public func send(completion: Subscribers.Completion<Failure>) { _complete(completion) }
    /// Exposes the wrapped subject as a read-only `Publisher`.
    public func eraseToPublisher() -> Publisher<Output, Failure> { _erase() }
}
