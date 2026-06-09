// Opens the `any Actor` existential to satisfy `isolated A`, running `body` on
// that actor's executor. Swift 5.7+ opens existentials for generic calls (SE-0352).
private func _hop<T: Sendable>(to actor: any Actor, _ body: @Sendable () -> T) async -> T {
    func run<A: Actor>(on a: isolated A) -> T { body() }
    return await run(on: actor)
}

// MARK: - receive(on:) / subscribe(on:)

extension Publisher {
    // Delivers values and completion to downstream on the specified actor's executor.
    public func receive(on actor: any Actor) -> Publisher<Output, Failure> {
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

    // Performs the upstream subscription (factory call) on the specified actor's executor.
    public func subscribe(on actor: any Actor) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    let upstream = await _hop(to: actor) { selfFactory() }
                    for await result in upstream {
                        switch result {
                        case .success(let v):
                            if case .terminated = raw.yield(.success(v)) { return }
                        case .failure(let e):
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
