// SPDX-License-Identifier: Apache-2.0
/// Records a fixed list of values plus a completion, replaying them cold on each subscription.
/// Mirrors Combine's `Record<Output, Failure>`.
public struct Record<Output: Sendable, Failure: Error>: Sendable {
    /// The captured values and terminating completion to be replayed.
    public struct Recording: Sendable {
        /// The values, replayed in order.
        public var output: [Output]
        /// The completion emitted after the values.
        public var completion: Subscribers.Completion<Failure>

        /// Creates a recording from a list of values and a completion (defaults to `.finished`).
        public init(
            output: [Output] = [],
            completion: Subscribers.Completion<Failure> = .finished
        ) {
            self.output = output
            self.completion = completion
        }

        /// Appends a value to the recording.
        public mutating func receive(_ value: Output) {
            output.append(value)
        }

        /// Sets the recording's terminating completion.
        public mutating func receive(completion: Subscribers.Completion<Failure>) {
            self.completion = completion
        }
    }

    /// The captured recording this publisher replays.
    public let recording: Recording

    /// Creates a record from an existing recording.
    public init(recording: Recording) {
        self.recording = recording
    }

    /// Creates a record by building its recording imperatively in a closure.
    public init(_ record: (inout Recording) -> Void) {
        var r = Recording()
        record(&r)
        recording = r
    }

    /// Exposes the recording as a cold publisher that replays it on each subscription.
    public func eraseToPublisher() -> Publisher<Output, Failure> {
        let output = recording.output
        let completion = recording.completion
        return Publisher<Output, Failure> { continuation in
            for value in output {
                continuation.yield(value)
            }
            switch completion {
            case .finished: continuation.finish()
            case let .failure(error): continuation.fail(error)
            }
        }
    }
}
