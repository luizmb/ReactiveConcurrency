import ReactiveConcurrency

let query = PassthroughSubject<String, Never>()

// Wait for a 300 ms pause in typing, and ignore consecutive duplicates.
let stableTerm = query.eraseToPublisher()
    .debounce(for: .milliseconds(300), clock: ContinuousClock())
    .removeDuplicates()
