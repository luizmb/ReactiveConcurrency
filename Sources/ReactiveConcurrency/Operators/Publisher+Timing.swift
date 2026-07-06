// SPDX-License-Identifier: Apache-2.0

import Hourglass

// Time-based operators delegate to Hourglass's AsyncStream operators. Hourglass works on
// AsyncStream<Element> (not AsyncSequence) precisely so iteration can't surface an untyped
// `any Error`; our typed Failure channel is preserved by `_timed`, which routes successes
// through the Hourglass transform and forwards a failure immediately, terminating the stream.

public extension Publisher {
    /// Delays forwarding elements (and completion) by `interval`. Failures are delayed too,
    /// matching Combine — the whole Result stream is shifted by Hourglass's `delay`.
    func delay<C: Clock & Sendable>(
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
    func debounce<C: Clock & Sendable>(
        for interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<Output, Failure> {
        _timed { $0.debounce(for: interval, clock: clock) }
    }

    /// Emits the first value in each `interval` window (leading edge) when `latest` is false,
    /// or the most recent value at the end of each window when `latest` is true.
    func throttle<C: Clock & Sendable>(
        for interval: C.Instant.Duration,
        clock: C,
        latest: Bool
    ) -> Publisher<Output, Failure> {
        _timed { $0.throttle(for: interval, clock: clock, latest: latest) }
    }

    /// Replaces each value with the elapsed duration since the previous value (or subscription).
    func measureInterval<C: Clock & Sendable>(
        using clock: C
    ) -> Publisher<C.Instant.Duration, Failure> {
        _timed { $0.measureInterval(using: clock) }
    }

    /// Groups values into arrays, flushing at the end of each time window. Empty windows are
    /// skipped; a partial window is flushed when the upstream completes.
    func collect<C: Clock & Sendable>(
        every interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<[Output], Failure> {
        _timed { $0.collect(every: interval, clock: clock) }
    }

    /// Groups values into arrays, flushing when the time window elapses OR the buffer reaches
    /// `count`, whichever comes first; each flush resets the window. A partial window is flushed
    /// when the upstream completes.
    func collect<C: Clock & Sendable>(
        every interval: C.Instant.Duration,
        orCount count: Int,
        clock: C
    ) -> Publisher<[Output], Failure> {
        _timed { $0.collect(every: interval, orCount: count, clock: clock) }
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
                        case let .success(value):
                            valuesContinuation.yield(value)
                        case let .failure(error):
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

public extension Publisher where Failure: Error {
    /// Fails with `error` if no value arrives within `interval` of subscription or the last value.
    ///
    /// Bridges Hourglass's `AsyncStream.timeout`, which carries the timeout as a typed
    /// `Result<Output, Failure>` value (never a thrown `any Error`). The success channel feeds
    /// the Hourglass operator; an upstream failure preempts the timer and terminates immediately.
    func timeout<C: Clock & Sendable>(
        _ interval: C.Instant.Duration,
        clock: C,
        error: @autoclosure @escaping @Sendable () -> Failure
    ) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>> { downstream in
                let (values, valuesContinuation) = AsyncStream<Output>.makeStream()

                // Hourglass timeout over the success channel emits Result directly downstream.
                let consumer = Task {
                    for await result in values.timeout(interval, clock: clock, error: error) {
                        if case .terminated = downstream.yield(result) { return }
                        if case .failure = result { downstream.finish(); return }
                    }
                    downstream.finish()
                }
                let producer = Task {
                    for await result in upstream {
                        switch result {
                        case let .success(value):
                            valuesContinuation.yield(value)
                        case let .failure(upstreamError):
                            consumer.cancel()
                            valuesContinuation.finish()
                            _ = downstream.yield(.failure(upstreamError))
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
