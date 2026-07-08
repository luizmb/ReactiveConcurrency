import ReactiveConcurrency

// At the boundary, the error is a value in the completion — or a Result via `.results`.
let cancellable = load.sink(
    receiveCompletion: { completion in
        if case let .failure(error) = completion { report(error) }
    },
    receiveValue: { data in use(data) }
)

// Or iterate results, where each element is Result<Data, LoadError>:
for await result in load.results {
    print(result)
}
