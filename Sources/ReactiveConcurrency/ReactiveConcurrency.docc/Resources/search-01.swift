import ReactiveConcurrency

// Each keystroke flows into a hot subject.
let query = PassthroughSubject<String, Never>()
