// assign(to:on:) — write each emitted value into a property via key path.
//
// Combine's assign predates strict concurrency; here the write must be isolation-safe. Two
// shapes cover the cases:
//   - portable: the object promises its own thread-safety (Root: Sendable), written on the
//     consumer task — works on every platform / server-side.
//   - @MainActor: the object is main-actor isolated (UI), written on the main actor in order.

extension Publisher where Failure == Never {
    /// Writes each value into `object[keyPath:]`. `Root: Sendable` because the write happens on
    /// the subscription's task — the object is responsible for its own synchronization.
    public func assign<Root: AnyObject & Sendable>(
        to keyPath: ReferenceWritableKeyPath<Root, Output> & Sendable,
        on object: Root
    ) -> AnyCancellable {
        sink { [weak object] value in object?[keyPath: keyPath] = value }
    }

    /// Writes each value into a main-actor-isolated `object` on the main actor, in order.
    /// The natural binding for UI: `publisher.assign(to: \.text, on: label)`.
    @MainActor
    public func assignOnMain<Root: AnyObject>(
        to keyPath: ReferenceWritableKeyPath<Root, Output>,
        on object: Root
    ) -> AnyCancellable {
        let stream = values
        let task = Task { @MainActor in
            for await value in stream { object[keyPath: keyPath] = value }
        }
        return AnyCancellable { task.cancel() }
    }
}

// MARK: - Failable: assign the Result events
//
// A failable publisher has no plain value to assign on failure, so the target is a
// `Result<Output, Failure>` property: each element writes `.success(value)`, and a failure
// completion writes `.failure(error)`. (Result is a value type — only `Root` must be a reference.)

extension Publisher {
    /// Writes each event as a `Result` into `object[keyPath:]` (`.success` per value,
    /// `.failure` on failure). `Root: Sendable`; written on the subscription task.
    public func assign<Root: AnyObject & Sendable>(
        to keyPath: ReferenceWritableKeyPath<Root, Result<Output, Failure>> & Sendable,
        on object: Root
    ) -> AnyCancellable {
        sink(
            receiveCompletion: { [weak object] completion in
                if case .failure(let error) = completion { object?[keyPath: keyPath] = .failure(error) }
            },
            receiveValue: { [weak object] value in object?[keyPath: keyPath] = .success(value) }
        )
    }

    /// Writes each event as a `Result` into a main-actor-isolated `object`, on the main actor,
    /// in order.
    @MainActor
    public func assignOnMain<Root: AnyObject>(
        to keyPath: ReferenceWritableKeyPath<Root, Result<Output, Failure>>,
        on object: Root
    ) -> AnyCancellable {
        let stream = results
        let task = Task { @MainActor in
            for await result in stream { object[keyPath: keyPath] = result }
        }
        return AnyCancellable { task.cancel() }
    }
}
