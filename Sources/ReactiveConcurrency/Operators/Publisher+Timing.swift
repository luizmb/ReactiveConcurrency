extension Publisher {
    public func debounce<C: Clock>(
        for interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    var pendingTask: Task<Void, Never>?
                    defer { pendingTask?.cancel() }
                    for await result in upstream {
                        switch result {
                        case .success(let value):
                            pendingTask?.cancel()
                            pendingTask = Task {
                                try? await clock.sleep(until: clock.now.advanced(by: interval), tolerance: nil)
                                guard !Task.isCancelled else { return }
                                _ = raw.yield(Result.success(value))
                            }
                        case .failure(let error):
                            _ = raw.yield(Result.failure(error)); raw.finish(); return
                        }
                    }
                    if !Task.isCancelled, let pending = pendingTask {
                        await pending.value
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    public func delay<C: Clock>(
        for interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success(let value):
                            try? await clock.sleep(until: clock.now.advanced(by: interval), tolerance: nil)
                            guard !Task.isCancelled else { return }
                            if case .terminated = raw.yield(Result.success(value)) { return }
                        case .failure(let error):
                            _ = raw.yield(Result.failure(error)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    // Emits the first value in each interval window (leading edge) when latest=false,
    // or the most recent value at the end of each window when latest=true.
    public func throttle<C: Clock>(
        for interval: C.Instant.Duration,
        clock: C,
        latest: Bool
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    var windowStart = clock.now
                    var latestValue: Output? = nil
                    var hasEmittedInWindow = false

                    for await result in upstream {
                        switch result {
                        case .success(let value):
                            let now = clock.now
                            if now >= windowStart.advanced(by: interval) {
                                if latest, let v = latestValue, hasEmittedInWindow {
                                    if case .terminated = raw.yield(Result.success(v)) { return }
                                }
                                windowStart = now
                                hasEmittedInWindow = false
                                latestValue = value
                            } else {
                                latestValue = value
                            }
                            if !hasEmittedInWindow && !latest {
                                if case .terminated = raw.yield(Result.success(value)) { return }
                                hasEmittedInWindow = true
                            } else if !hasEmittedInWindow && latest {
                                hasEmittedInWindow = true
                            }
                        case .failure(let error):
                            _ = raw.yield(Result.failure(error)); raw.finish(); return
                        }
                    }
                    if latest, let v = latestValue, hasEmittedInWindow {
                        _ = raw.yield(Result.success(v))
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

extension Publisher {
    public func measureInterval<C: Clock>(using clock: C) -> Publisher<C.Instant.Duration, Failure> {
        let selfFactory = _stream.factory
        return Publisher<C.Instant.Duration, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<C.Instant.Duration, Failure>> { raw in
                let task = Task {
                    var last = clock.now
                    for await result in upstream {
                        switch result {
                        case .success:
                            let now = clock.now
                            let interval = last.duration(to: now)
                            last = now
                            if case .terminated = raw.yield(.success(interval)) { return }
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

// MARK: - collect(every:clock:)

extension Publisher {
    /// Groups values into arrays, flushing at the end of each time window.
    /// A partial window is flushed when the upstream completes.
    public func collect<C: Clock>(every interval: C.Duration, clock: C) -> Publisher<[Output], Failure> {
        let selfFactory = _stream.factory
        return Publisher<[Output], Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<[Output], Failure>> { raw in
                let task = Task {
                    let upstreamBox = _StreamBox<Result<Output, Failure>>(upstream)

                    let (timerStream, timerCont) = AsyncStream<Void>.makeStream()
                    let timerTask = Task {
                        var next = clock.now.advanced(by: interval)
                        while !Task.isCancelled {
                            try? await clock.sleep(until: next, tolerance: nil)
                            guard !Task.isCancelled else { return }
                            timerCont.yield(())
                            next = next.advanced(by: interval)
                        }
                    }
                    let timerBox = _StreamBox<Void>(timerStream)

                    var bucket: [Output] = []
                    typealias _Ev = _CollectEvent<Output, Failure>
                    await withTaskGroup(of: _Ev.self) { group in
                        group.addTask { if let r = await upstreamBox.next() { .value(r) } else { .upstreamDone } }
                        group.addTask { (await timerBox.next()) != nil ? .tick : .timerDone }

                        loop: while let event = await group.next() {
                            switch event {
                            case .tick:
                                if !bucket.isEmpty {
                                    if case .terminated = raw.yield(.success(bucket)) { return }
                                    bucket = []
                                }
                                group.addTask { (await timerBox.next()) != nil ? .tick : .timerDone }
                            case .value(.success(let v)):
                                bucket.append(v)
                                group.addTask { if let r = await upstreamBox.next() { .value(r) } else { .upstreamDone } }
                            case .value(.failure(let e)):
                                timerTask.cancel(); timerCont.finish()
                                _ = raw.yield(.failure(e)); raw.finish(); return
                            case .upstreamDone:
                                timerTask.cancel(); timerCont.finish()
                                break loop
                            case .timerDone:
                                break loop
                            }
                        }
                    }
                    timerTask.cancel(); timerCont.finish()
                    if !bucket.isEmpty { _ = raw.yield(.success(bucket)) }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}

private enum _CollectEvent<V: Sendable, E: Error>: Sendable {
    case tick
    case value(Result<V, E>)
    case upstreamDone
    case timerDone
}

// MARK: - timeout

extension Publisher where Failure: Error {
    // Emits a failure if no value arrives within `interval` of subscription or the last value.
    public func timeout<C: Clock>(
        _ interval: C.Instant.Duration,
        clock: C,
        error: Failure
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    var timerTask: Task<Void, Never>? = Task {
                        try? await clock.sleep(until: clock.now.advanced(by: interval), tolerance: nil)
                        guard !Task.isCancelled else { return }
                        _ = raw.yield(Result.failure(error))
                        raw.finish()
                    }
                    defer { timerTask?.cancel() }
                    for await result in upstream {
                        timerTask?.cancel()
                        switch result {
                        case .success(let v):
                            if case .terminated = raw.yield(Result.success(v)) { return }
                            timerTask = Task {
                                try? await clock.sleep(until: clock.now.advanced(by: interval), tolerance: nil)
                                guard !Task.isCancelled else { return }
                                _ = raw.yield(Result.failure(error))
                                raw.finish()
                            }
                        case .failure(let e):
                            _ = raw.yield(Result.failure(e)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
