// SPDX-License-Identifier: Apache-2.0

public extension Publisher {
    /// Maps each element to a publisher and flattens their outputs, bounding concurrency to `maxPublishers`.
    ///
    /// Upstream consumption pauses when all slots are taken, providing natural backpressure.
    /// - Parameters:
    ///   - maxPublishers: The maximum number of inner publishers active at once.
    ///   - transform: A closure mapping each element to an inner publisher.
    /// - Returns: A publisher that emits the flattened inner outputs in arrival order.
    func flatMap<T: Sendable>(
        maxPublishers: Int,
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> Publisher<T, Failure> {
        let selfFactory = _stream.factory
        return Publisher<T, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<T, Failure>> { raw in
                let task = Task {
                    let (slots, slotsCont) = AsyncStream<Void>.makeStream(
                        bufferingPolicy: .bufferingNewest(maxPublishers)
                    )
                    for _ in 0..<maxPublishers {
                        slotsCont.yield(())
                    }
                    let slotsBox = StreamBox<Void>(slots)

                    await withTaskCancellationHandler {
                        await withTaskGroup(of: Void.self) { group in
                            outer: for await result in upstream {
                                switch result {
                                case let .success(value):
                                    guard await slotsBox.next() != nil else { break outer }
                                    guard !Task.isCancelled else { break outer }
                                    let inner = transform(value)._stream.factory()
                                    group.addTask {
                                        defer { slotsCont.yield(()) }
                                        for await r in inner {
                                            if case .terminated = raw.yield(r) { return }
                                            if case .failure = r { raw.finish(); return }
                                        }
                                    }
                                case let .failure(e):
                                    _ = raw.yield(.failure(e)); raw.finish(); break outer
                                }
                            }
                            for await _ in group {}
                        }
                        raw.finish()
                    } onCancel: {
                        slotsCont.finish() // unblock any waiting slotsBox.next()
                    }
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

public extension Publisher {
    /// Maps each element to a publisher and flattens all of their outputs, with unbounded concurrency.
    ///
    /// All inner publishers run concurrently; the first failure (upstream or any inner) seals the downstream.
    /// - Parameter transform: A closure mapping each element to an inner publisher.
    /// - Returns: A publisher that emits the flattened inner outputs in arrival order.
    func flatMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> Publisher<T, Failure> {
        let selfFactory = _stream.factory
        return Publisher<T, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<T, Failure>> { raw in
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        outer: for await result in upstream {
                            switch result {
                            case let .success(value):
                                let inner = transform(value)
                                group.addTask {
                                    for await innerResult in inner._stream.factory() {
                                        let yr = raw.yield(innerResult)
                                        if case .terminated = yr { return }
                                        if case .failure = innerResult { raw.finish(); return }
                                    }
                                }
                            case let .failure(error):
                                _ = raw.yield(Result.failure(error))
                                raw.finish()
                                break outer
                            }
                        }
                        for await _ in group {}
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    /// Flattens a publisher of publishers, forwarding only the most recent inner publisher's elements.
    ///
    /// Cancels the current inner publisher whenever a new upstream value arrives.
    /// - Returns: A publisher that emits values from the latest inner publisher.
    func switchToLatest<T: Sendable>() -> Publisher<T, Failure>
    where Output == Publisher<T, Failure> {
        _operator { raw, upstream in
            var innerTask: Task<Void, Never>?
            for await result in upstream {
                switch result {
                case let .success(inner):
                    innerTask?.cancel()
                    innerTask = Task {
                        for await innerResult in inner._stream.factory() {
                            guard !Task.isCancelled else { return }
                            let yr = raw.yield(innerResult)
                            if case .terminated = yr { return }
                            if case .failure = innerResult { raw.finish(); return }
                        }
                    }
                case let .failure(error):
                    innerTask?.cancel()
                    _ = raw.yield(Result.failure(error))
                    raw.finish()
                    return
                }
            }
            // Outer completed normally: let the still-running latest inner finish delivering its
            // values before completing (Combine completes only after the outer AND the last inner
            // complete). Cancelling here — as the old code did — silently dropped in-flight values.
            await innerTask?.value
            raw.finish()
        }
    }
}
