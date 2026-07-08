// SPDX-License-Identifier: Apache-2.0

public extension DeferredStream {
    /// Maps each element to a stream and concatenates them in order (concatMap; the monadic `bind`).
    ///
    /// This is the sequential, lawful product — use it for the cartesian product across streams.
    func flatMap<B: Sendable>(_ fn: @escaping @Sendable (Element) -> DeferredStream<B>) -> DeferredStream<B> {
        let outerFactory = factory
        return DeferredStream<B> {
            let upstream = outerFactory()
            return AsyncStream<B> { continuation in
                let task = Task { @Sendable in
                    for await element in upstream {
                        for await b in fn(element) {
                            continuation.yield(b)
                        }
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// The curried, free-function form of `flatMap` for point-free composition.
    static func flatMap<B: Sendable>(
        _ fn: @escaping @Sendable (Element) -> DeferredStream<B>
    ) -> @Sendable (DeferredStream<Element>) -> DeferredStream<B> {
        { @Sendable stream in stream.flatMap(fn) }
    }

    /// Concatenates two streams: yields all of `lhs`, then all of `rhs`. Forms a monoid with ``empty()``.
    static func alt(_ lhs: DeferredStream<Element>, _ rhs: @autoclosure () -> DeferredStream<Element>) -> DeferredStream<Element> {
        let lhsFactory = lhs.factory
        let rhsFactory = rhs().factory
        return DeferredStream<Element> {
            let lhsStream = lhsFactory()
            let rhsStream = rhsFactory()
            return AsyncStream<Element> { continuation in
                let task = Task { @Sendable in
                    for await element in lhsStream {
                        continuation.yield(element)
                    }
                    for await element in rhsStream {
                        continuation.yield(element)
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// The empty stream (finishes immediately with no elements); the identity for ``alt(_:_:)``.
    static func empty() -> DeferredStream<Element> {
        DeferredStream<Element> { AsyncStream { $0.finish() } }
    }

    /// Flattens a stream of streams into a single stream (monadic `join`).
    static func join<A: Sendable>(_ nested: DeferredStream<DeferredStream<A>>) -> DeferredStream<A>
    where Element == DeferredStream<A> {
        nested.flatMap { $0 }
    }

    /// Discards each element's value, yielding a `DeferredStream<Void>`.
    func void() -> DeferredStream<Void> {
        map { _ in }
    }

    /// Left-to-right Kleisli composition: `f` then `g`, threading the stream through both.
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Element) -> DeferredStream<B>,
        _ g: @escaping @Sendable (B) -> DeferredStream<C>
    ) -> @Sendable (Element) -> DeferredStream<C> {
        { @Sendable a in f(a).flatMap(g) }
    }

    /// Right-to-left Kleisli composition: applies `f` first, then `g`.
    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Element) -> DeferredStream<B>,
        _ f: @escaping @Sendable (X) -> DeferredStream<Element>
    ) -> @Sendable (X) -> DeferredStream<B> {
        { @Sendable x in f(x).flatMap(g) }
    }
}
