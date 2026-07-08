import ReactiveConcurrency

let query = PassthroughSubject<String, Never>()

// Map each settled term to a search, and switch to the latest —
// switchToLatest cancels the previous in-flight search automatically.
let results = query.eraseToPublisher()
    .debounce(for: .milliseconds(300), clock: ContinuousClock())
    .removeDuplicates()
    .map { term in search(term) } // Publisher<[Hit], Never> per term
    .switchToLatest()
