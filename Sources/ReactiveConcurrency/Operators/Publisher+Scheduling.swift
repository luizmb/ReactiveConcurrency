// SPDX-License-Identifier: Apache-2.0

// Opens the `any Actor` existential to satisfy `isolated A`, running `body` on
// that actor's executor. Swift 5.7+ opens existentials for generic calls (SE-0352).
private func _hop<T: Sendable>(to actor: any Actor, _ body: @Sendable () -> T) async -> T {
    func run<A: Actor>(on a: isolated A) -> T { body() }
    return await run(on: actor)
}

// MARK: - receive(on:) / subscribe(on:)

public extension Publisher {
    /// Delivers values and completion downstream on `actor`'s executor. Takes an `any Actor`
    /// (there is no `Scheduler` abstraction) — pass any actor, including `MainActor.shared`.
    func receive(on actor: any Actor) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    for await result in upstream {
                        let yr = await _hop(to: actor) { raw.yield(result) }
                        switch result {
                        case .failure:
                            await _hop(to: actor) { raw.finish() }; return
                        case .success:
                            if case .terminated = yr { return }
                        }
                    }
                    await _hop(to: actor) { raw.finish() }
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    /// Performs the upstream subscription (the factory call) on `actor`'s executor. Takes an
    /// `any Actor` (no `Scheduler` abstraction) rather than a scheduler.
    func subscribe(on actor: any Actor) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    let upstream = await _hop(to: actor) { selfFactory() }
                    for await result in upstream {
                        switch result {
                        case let .success(v):
                            if case .terminated = raw.yield(.success(v)) { return }
                        case let .failure(e):
                            _ = raw.yield(.failure(e)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
