import ReactiveConcurrency

// A cold, expensive publisher: it re-runs from scratch for EACH subscriber.
let expensive = Publisher<Int, Never> { continuation in
    let value = await runExpensiveComputation()
    continuation.yield(value)
    continuation.finish()
}
