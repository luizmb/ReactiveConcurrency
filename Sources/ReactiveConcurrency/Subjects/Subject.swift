// SPDX-License-Identifier: Apache-2.0

public protocol Subject<Output, Failure>: AnyObject, Sendable {
    associatedtype Output: Sendable
    associatedtype Failure: Error

    func send(_ value: Output)
    func send(completion: Subscribers.Completion<Failure>)
    func eraseToPublisher() -> Publisher<Output, Failure>
}

public extension Subject {
    func eraseToAnySubject() -> AnySubject<Output, Failure> {
        AnySubject(self)
    }
}

public struct AnySubject<Output: Sendable, Failure: Error>: Sendable {
    private let _send: @Sendable (Output) -> Void
    private let _complete: @Sendable (Subscribers.Completion<Failure>) -> Void
    private let _erase: @Sendable () -> Publisher<Output, Failure>

    public init<S: Subject>(_ subject: S) where S.Output == Output, S.Failure == Failure {
        _send = { subject.send($0) }
        _complete = { subject.send(completion: $0) }
        _erase = { subject.eraseToPublisher() }
    }

    public func send(_ value: Output) { _send(value) }
    public func send(completion: Subscribers.Completion<Failure>) { _complete(completion) }
    public func eraseToPublisher() -> Publisher<Output, Failure> { _erase() }
}
