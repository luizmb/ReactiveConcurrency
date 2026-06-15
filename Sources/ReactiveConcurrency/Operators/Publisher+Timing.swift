import Hourglass

// Time-based operators delegate to Hourglass's AsyncStream operators. Hourglass works on
// AsyncStream<Element> (not AsyncSequence) precisely so iteration can't surface an untyped
// `any Error`; our typed Failure channel is preserved by `_timed`, which routes successes
// through the Hourglass transform and forwards a failure immediately, terminating the stream.

extension Publisher {
    /// Delays forwarding elements (and completion) by `interval`. Failures are delayed too,
    /// matching Combine — the whole Result stream is shifted by Hourglass's `delay`.
    public func delay<C: Clock & Sendable>(
        for interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            selfFactory().delay(for: interval, clock: clock)
        })
    }

    /// Emits a value only after the upstream has been quiet for `interval`; the timer resets
    /// on each new value. A failure preempts any pending value and terminates immediately.
    public func debounce<C: Clock & Sendable>(
        for interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<Output, Failure> {
        _timed { $0.debounce(for: interval, clock: clock) }
    }

    /// Emits the first value in each `interval` window (leading edge) when `latest` is false,
    /// or the most recent value at the end of each window when `latest` is true.
    public func throttle<C: Clock & Sendable>(
        for interval: C.Instant.Duration,
        clock: C,
        latest: Bool
    ) -> Publisher<Output, Failure> {
        _timed { $0.throttle(for: interval, clock: clock, latest: latest) }
    }

    /// Replaces each value with the elapsed duration since the previous value (or subscription).
    public func measureInterval<C: Clock & Sendable>(
        using clock: C
    ) -> Publisher<C.Instant.Duration, Failure> {
        _timed { $0.measureInterval(using: clock) }
    }

    /// Groups values into arrays, flushing at the end of each time window. Empty windows are
    /// skipped; a partial window is flushed when the upstream completes.
    public func collect<C: Clock & Sendable>(
        every interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<[Output], Failure> {
        _timed { $0.collect(every: interval, clock: clock) }
    }
}

// MARK: - value-timing bridge

extension Publisher {
    // Runs a Hourglass value-timing transform over the success channel. Successes are routed
    // into a plain AsyncStream<Output> for `transform`; its output is re-wrapped as `.success`.
    // A failure bypasses the transform: it cancels the consumer (dropping any pending timed
    // value, as Combine does) and forwards `.failure` immediately. Normal completion lets the
    // transform drain (flushing e.g. a debounce/throttle/collect tail) before finishing.
    func _timed<T: Sendable>(
        _ transform: @escaping @Sendable (AsyncStream<Output>) -> AsyncStream<T>
    ) -> Publisher<T, Failure> {
        let selfFactory = _stream.factory
        return Publisher<T, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<T, Failure>> { downstream in
                let (values, valuesContinuation) = AsyncStream<Output>.makeStream()

                let consumer = Task {
                    for await value in transform(values) {
                        if case .terminated = downstream.yield(.success(value)) { return }
                    }
                    downstream.finish()
                }
                let producer = Task {
                    for await result in upstream {
                        switch result {
                        case .success(let value):
                            valuesContinuation.yield(value)
                        case .failure(let error):
                            consumer.cancel()
                            valuesContinuation.finish()
                            _ = downstream.yield(.failure(error))
                            downstream.finish()
                            return
                        }
                    }
                    valuesContinuation.finish()
                }
                downstream.onTermination = { _ in
                    producer.cancel()
                    consumer.cancel()
                    valuesContinuation.finish()
                }
            }
        })
    }
}

// MARK: - timeout

extension Publisher where Failure: Error {
    // Emits `error` if no value arrives within `interval` of subscription or the last value.
    // No Hourglass equivalent exists (timeout injects into the failure channel), so it is timed
    // directly off the clock; `try?` on clock.sleep only absorbs cancellation, which is then
    // re-checked via Task.isCancelled — no real error is swallowed.
    public func timeout<C: Clock & Sendable>(
        _ interval: C.Instant.Duration,
        clock: C,
        error: Failure
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    @Sendable func armTimer() -> Task<Void, Never> {
                        Task {
                            try? await clock.sleep(until: clock.now.advanced(by: interval), tolerance: nil)
                            guard !Task.isCancelled else { return }
                            _ = raw.yield(.failure(error))
                            raw.finish()
                        }
                    }
                    var timerTask = armTimer()
                    defer { timerTask.cancel() }
                    for await result in upstream {
                        timerTask.cancel()
                        switch result {
                        case .success(let value):
                            if case .terminated = raw.yield(.success(value)) { return }
                            timerTask = armTimer()
                        case .failure(let error):
                            _ = raw.yield(.failure(error)); raw.finish(); return
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
