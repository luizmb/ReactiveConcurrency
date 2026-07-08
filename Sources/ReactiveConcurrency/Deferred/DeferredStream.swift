// SPDX-License-Identifier: Apache-2.0

// DeferredStream<Element>: a lazy AsyncSequence whose producer starts only at first iteration.
// Contrast with AsyncStream: its body/Task runs at init time.
// DeferredStream defers the factory call to makeAsyncIterator().
///
/// Unlike `AsyncStream`, whose body closure runs at initialisation time, `DeferredStream`
/// defers the factory call to `makeAsyncIterator()`. This means:
///
/// - No work is done until a `for await` loop begins.
/// - Each new iterator (each `for await` loop) calls the factory again, creating a fresh
///   `AsyncStream` — enabling true restartable or re-subscribable streams.
///
/// `DeferredStream` is the Swift async/await counterpart to Combine's `Deferred<P>`.
/// It is a full functor (``map(_:)``), applicative (``applyDeferredStream(_:_:)``), and monad
/// (`flatMap`) — operator forms available in `CoreFPOperators`.
///
/// Note its applicative is *zippy* (ZipList-style, positional, truncating to the shortest input),
/// so ``applyDeferredStream(_:_:)`` / `liftA2` / `zip` are not the monad-derived product; use
/// `flatMap` (concatMap) for the cartesian product.
///
/// ## Creating a DeferredStream
///
/// ```swift
/// // Lazy: the body closure does not execute until iteration starts.
/// let stream = DeferredStream {
///     AsyncStream<Int> { continuation in
///         Task {
///             for i in 0..<5 {
///                 continuation.yield(i)
///             }
///             continuation.finish()
///         }
///     }
/// }
///
/// // Wrapping an already-created stream (use with care — iteration starts immediately
/// // on the wrapped stream the first time, not on each new iterator):
/// let wrapped = DeferredStream.wrap(existingStream)
/// ```
///
/// ## Functor / Monad operations
///
/// ```swift
/// let doubled = stream.map { $0 * 2 }          // DeferredStream<Int>
/// let filtered = stream.flatMap { n in          // DeferredStream<String>
///     DeferredStream { AsyncStream.just("\(n)") }
/// }
/// ```
///
/// - SeeAlso: ``DeferredTask``, `AsyncStream`

/// A lazy `AsyncSequence` whose producer is created only when iteration begins.
public struct DeferredStream<Element: Sendable>: AsyncSequence, Sendable {
    public typealias AsyncIterator = AsyncStream<Element>.AsyncIterator

    /// Builds a fresh `AsyncStream` on each call; invoked once per ``makeAsyncIterator()``.
    public let factory: @Sendable () -> AsyncStream<Element>

    /// Wraps a stream-producing factory. The factory is not called until iteration begins.
    public init(_ factory: @escaping @Sendable () -> AsyncStream<Element>) {
        self.factory = factory
    }

    /// Invokes the factory to produce a fresh underlying stream and returns its iterator.
    public func makeAsyncIterator() -> AsyncIterator {
        factory().makeAsyncIterator()
    }
}

public extension DeferredStream {
    /// Wraps an existing eager `AsyncStream` (already created; use only when you own it).
    static func wrap(_ stream: AsyncStream<Element>) -> DeferredStream<Element> {
        DeferredStream { stream }
    }
}
