import ReactiveConcurrency

// Retry a few times on failure, then fall back to a default value.
let resilient = load
    .retry(2)
    .replaceError(with: Data())
