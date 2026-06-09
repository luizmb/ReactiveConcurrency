// A publisher that records values and a completion, then replays them cold on each subscription.
// Mirrors Combine's Record<Output, Failure>.
public struct Record<Output: Sendable, Failure: Error>: Sendable {
    public struct Recording: Sendable {
        public var output: [Output]
        public var completion: Subscribers.Completion<Failure>

        public init(
            output: [Output] = [],
            completion: Subscribers.Completion<Failure> = .finished
        ) {
            self.output = output
            self.completion = completion
        }

        public mutating func receive(_ value: Output) {
            output.append(value)
        }

        public mutating func receive(completion: Subscribers.Completion<Failure>) {
            self.completion = completion
        }
    }

    public let recording: Recording

    public init(recording: Recording) {
        self.recording = recording
    }

    // Build a recording in a closure.
    public init(_ record: (inout Recording) -> Void) {
        var r = Recording()
        record(&r)
        self.recording = r
    }

    public func eraseToPublisher() -> Publisher<Output, Failure> {
        let output = recording.output
        let completion = recording.completion
        return Publisher<Output, Failure> { continuation in
            for value in output { continuation.yield(value) }
            switch completion {
            case .finished: continuation.finish()
            case .failure(let error): continuation.fail(error)
            }
        }
    }
}
