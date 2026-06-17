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

extension Publisher {
    public func buffer(size: Int, whenFull strategy: BufferStrategy) -> Publisher<Output, Failure> {
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
                        if case .terminated = raw.yield(result) { return }
                        if case .failure = result { raw.finish(); return }
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }
}
