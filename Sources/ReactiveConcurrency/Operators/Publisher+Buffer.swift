// SPDX-License-Identifier: Apache-2.0

// buffer(size:whenFull:) — bound how many undelivered elements are held, dropping per strategy
// when the downstream can't keep up. Maps to AsyncStream's buffering policy.
//
// Combine's `prefetch` is omitted (it's a demand concept; this library is pull-based with no
// backpressure-demand model), as is `.customError` (AsyncStream exposes no overflow hook).

public enum BufferStrategy: Sendable {
    /// Drop the oldest buffered element to make room for a new one (keep newest `size`).
    case dropOldest
    /// Drop the incoming element when the buffer is full (keep oldest `size`).
    case dropNewest
}

public extension Publisher {
    func buffer(size: Int, whenFull strategy: BufferStrategy) -> Publisher<Output, Failure> {
        let selfFactory = _stream.factory
        let policy: AsyncStream<Result<Output, Failure>>.Continuation.BufferingPolicy =
            switch strategy {
            case .dropOldest: .bufferingNewest(size)
            case .dropNewest: .bufferingOldest(size)
            }
        return Publisher<Output, Failure>(DeferredStream {
            let upstream = selfFactory()
            return AsyncStream<Result<Output, Failure>>(bufferingPolicy: policy) { raw in
                let task = Task {
                    for await result in upstream {
                        switch result {
                        case .success:
                            if case .terminated = raw.yield(result) { return }
                        case .failure:
                            // The terminal failure is a completion event and must not be dropped
                            // by the buffering policy (`.dropNewest` evicts an incoming element when
                            // the buffer is full — Combine never drops completions). Yield it
                            // cooperatively until the buffer accepts it (as the consumer drains,
                            // room frees) or the downstream goes away.
                            while true {
                                switch raw.yield(result) {
                                case .enqueued: raw.finish(); return
                                case .terminated: return
                                case .dropped: await Task.yield()
                                @unknown default: raw.finish(); return
                                }
                            }
                        }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
