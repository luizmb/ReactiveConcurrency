// SPDX-License-Identifier: Apache-2.0

// merge over an arbitrary number of publishers (Combine's Publishers.MergeMany).
// All sources run concurrently; outputs interleave in arrival order; the first failure
// (from any source) seals the stream.

public extension Publisher {
    /// Interleaves the elements of an arbitrary number of publishers, emitting them in arrival order.
    ///
    /// All sources run concurrently; the first failure from any source seals the stream.
    /// - Parameter publishers: The publishers to merge.
    /// - Returns: A publisher that emits from every source.
    static func merge(_ publishers: [Publisher<Output, Failure>]) -> Publisher<Output, Failure> {
        let factories = publishers.map(\._stream.factory)
        return Publisher<Output, Failure>(DeferredStream {
            // Pre-subscribe all sources synchronously so values sent right after .sink() aren't lost.
            let streams = factories.map { $0() }
            return AsyncStream<Result<Output, Failure>> { raw in
                // Every child writes downstream: the gate keeps the first failure the last event.
                let gate = TerminalGate(raw)
                let task = Task {
                    await withTaskGroup(of: Void.self) { group in
                        for stream in streams {
                            group.addTask {
                                for await result in stream {
                                    switch result {
                                    case .success:
                                        guard gate.yield(result) else { return }
                                    case .failure:
                                        gate.finish(with: result); return
                                    }
                                }
                            }
                        }
                        await group.waitForAll()
                    }
                    gate.finish()
                }
                raw.onTermination = { _ in task.cancel() }
            }
        })
    }

    /// Interleaves this publisher with an array of others, emitting elements in arrival order.
    ///
    /// All sources run concurrently; the first failure from any source seals the stream.
    /// - Parameter others: The additional publishers to merge with this one.
    /// - Returns: A publisher that emits from this publisher and every other.
    func merge(with others: [Publisher<Output, Failure>]) -> Publisher<Output, Failure> {
        Publisher.merge([self] + others)
    }
}
