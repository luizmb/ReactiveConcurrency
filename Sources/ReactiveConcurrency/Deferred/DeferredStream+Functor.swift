// SPDX-License-Identifier: Apache-2.0

public extension DeferredStream {
    /// Transforms each element with `fn`, lazily, once iteration begins.
    func map<B: Sendable>(_ fn: @escaping @Sendable (Element) -> B) -> DeferredStream<B> {
        let outerFactory = factory
        return DeferredStream<B> {
            let upstream = outerFactory()
            return AsyncStream<B> { continuation in
                let task = Task { @Sendable in
                    for await element in upstream {
                        continuation.yield(fn(element))
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// The curried, free-function form of ``map(_:)`` for point-free composition.
    static func fmap<B: Sendable>(
        _ fn: @escaping @Sendable (Element) -> B
    ) -> @Sendable (DeferredStream<Element>) -> DeferredStream<B> {
        { @Sendable stream in stream.map(fn) }
    }

    /// Replaces every element with `value`, preserving the element count.
    func replace<B: Sendable>(_ value: B) -> DeferredStream<B> {
        map { _ in value }
    }
}
