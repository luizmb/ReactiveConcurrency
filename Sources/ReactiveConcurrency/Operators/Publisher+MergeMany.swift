// merge over an arbitrary number of publishers (Combine's Publishers.MergeMany).
// All sources run concurrently; outputs interleave in arrival order; the first failure
// (from any source) seals the stream.

extension Publisher {
    public static func merge(_ publishers: [Publisher<Output, Failure>]) -> Publisher<Output, Failure> {
        let factories = publishers.map(\._stream.factory)
        return Publisher<Output, Failure>(DeferredStream {
            // Pre-subscribe all sources synchronously so values sent right after .sink() aren't lost.
            let streams = factories.map { $0() }
            return AsyncStream<Result<Output, Failure>> { raw in
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        for stream in streams {
                            group.addTask {
                                for await result in stream {
                                    switch result {
                                    case .success:
                                        if case .terminated = raw.yield(result) { return }
                                    case .failure:
                                        _ = raw.yield(result); raw.finish(); return
                                    }
                                }
                            }
                        }
                        await group.waitForAll()
                    }
                    raw.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    public func merge(with others: [Publisher<Output, Failure>]) -> Publisher<Output, Failure> {
        Publisher.merge([self] + others)
    }
}
