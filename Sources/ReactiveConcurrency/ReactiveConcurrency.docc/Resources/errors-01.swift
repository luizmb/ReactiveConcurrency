import ReactiveConcurrency

enum LoadError: Error, Sendable { case offline, notFound }

// The failure type is part of the Publisher's type — no untyped `any Error`.
let load: Publisher<Data, LoadError> = Publisher { continuation in
    let data = try await fetch() // async throws(LoadError)
    continuation.yield(data)
    continuation.finish()
}
