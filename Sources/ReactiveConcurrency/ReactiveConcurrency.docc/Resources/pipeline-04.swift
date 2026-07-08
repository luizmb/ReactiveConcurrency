import ReactiveConcurrency

let pipeline = [1, 2, 3, 4, 5].publisher
    .filter { $0.isMultiple(of: 2) }
    .map { $0 * 10 }

// Or consume it as an AsyncSequence — the same pipeline, run a different way.
for await value in pipeline.values {
    print(value) // 20, then 40
}
