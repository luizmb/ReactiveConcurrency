// SPDX-License-Identifier: Apache-2.0

/// A cold, lazy, composable stream of `Output` values that may fail with a typed `Failure`.
///
/// `Publisher` is the heart of ReactiveConcurrency. If you know Combine you already know most of the
/// surface — `map`, `filter`, `combineLatest`, `sink`, subjects — but a `Publisher` here is a **pure,
/// lazy value**: composing operators just builds a bigger value, and nothing runs until you attach a
/// subscriber with ``sink(receiveValue:)`` or iterate it as an `AsyncSequence` via ``values``.
///
/// It is a concrete `struct` backed by a `DeferredStream<Result<Output, Failure>>` — not a protocol.
/// A `.failure` element is always the stream's last element. Everything is `Sendable`, and it runs on
/// Apple platforms, Linux, Windows, and Android.
///
/// ```swift
/// let pipeline = [1, 2, 3, 4, 5].publisher   // Publisher<Int, Never>
///     .filter { $0.isMultiple(of: 2) }       // → 2, 4
///     .map { $0 * 10 }                       // → 20, 40
///
/// let cancellable = pipeline.sink { print($0) }   // 20, then 40
/// ```
///
/// - SeeAlso: <doc:CoreConcepts>, <doc:GettingStarted>
///
/// ## Topics
///
/// ### Essentials
/// - <doc:CoreConcepts>
/// - ``init(_:)``
/// - ``Continuation``
///
/// ### Creating a Publisher
/// - ``just(_:)``
/// - ``empty()``
/// - ``fail(_:)``
/// - ``sequence(_:)``
/// - ``pure(_:)``
/// - ``eraseToAnyPublisher()``
///
/// ### Transforming Values
/// - <doc:TransformingValues>
/// - ``compactMap(_:)``
/// - ``mapError(_:)``
/// - ``scan(_:_:)``
/// - ``reduce(_:_:)``
/// - ``replaceNil(with:)``
/// - ``setFailureType(to:)``
/// - ``count()``
///
/// ### Filtering Values
/// - <doc:FilteringValues>
/// - ``filter(_:)``
/// - ``removeDuplicates()``
/// - ``removeDuplicates(by:)``
/// - ``ignoreOutput()``
/// - ``replaceEmpty(with:)``
/// - ``output(at:)``
/// - ``output(in:)``
/// - ``contains(_:)``
/// - ``allSatisfy(_:)``
///
/// ### Combining Publishers
/// - <doc:CombiningPublishers>
/// - ``switchToLatest()``
///
/// ### Controlling Timing
/// - <doc:ControllingTiming>
/// - ``delay(for:clock:)``
/// - ``debounce(for:clock:)``
/// - ``throttle(for:clock:latest:)``
/// - ``timeout(_:clock:error:)``
/// - ``measureInterval(using:)``
///
/// ### Handling Errors
/// - <doc:HandlingErrors>
/// - ``replaceError(with:)``
/// - ``retry(_:)``
/// - ``tryCatch(_:)``
///
/// ### Sharing & Multicasting
/// - <doc:SharingAndMulticasting>
/// - ``share()``
/// - ``makeConnectable()``
/// - ``buffer(size:whenFull:)``
///
/// ### Scheduling
/// - ``receive(on:)``
/// - ``subscribe(on:)``
///
/// ### Consuming a Publisher
/// - <doc:ConsumingPublishers>
/// - ``sink(receiveValue:)``
/// - ``firstValue()``
/// - ``firstResult()``
///
/// ### Bridging to AsyncSequence
/// - <doc:BridgingAsyncSequence>
/// - ``values``
/// - ``results``
///
/// ### Functional Algebra
/// - <doc:FunctionalAlgebra>
/// - <doc:MonadTransformers>
/// - ``void()``
/// - ``join()``
public struct Publisher<Output: Sendable, Failure: Error>: Sendable {
    let _stream: DeferredStream<Result<Output, Failure>>

    // Low-level init used by operators — direct DeferredStream wrapping.
    init(_ stream: DeferredStream<Result<Output, Failure>>) {
        _stream = stream
    }

    /// Creates a custom publisher from an async `body` that drives a ``Continuation``.
    ///
    /// The body runs in a `Task` when a subscriber attaches (cold). Yield values with
    /// ``Continuation/yield(_:)``, finish with ``Continuation/finish()``, and fail by throwing the
    /// typed `Failure` (or calling ``Continuation/fail(_:)``). Cancellation is cooperative — check
    /// ``Continuation/isCancelled`` in synchronous loops.
    ///
    /// ```swift
    /// let ticks = Publisher<Int, Never> { continuation in
    ///     for i in 0..<3 where !continuation.isCancelled { continuation.yield(i) }
    /// }
    /// ```
    public init(_ body: @escaping @Sendable (Continuation) async throws(Failure) -> Void) {
        _stream = DeferredStream {
            let (stream, raw) = AsyncStream<Result<Output, Failure>>.makeStream()
            let cont = Continuation(raw)
            let task = Task {
                // withTaskCancellationHandler uses untyped rethrows, so we can't let
                // throws(Failure) propagate through it directly. Wrap the outcome in
                // Result<Void, Failure> inside the non-throwing operation closure, then
                // call .get() outside — Result.get() is throws(Failure) in Swift 6.
                let outcome: Result<Void, Failure> = await withTaskCancellationHandler {
                    do throws(Failure) {
                        try await body(cont)
                        return .success(())
                    } catch {
                        return .failure(error)
                    }
                } onCancel: {
                    raw.finish()
                }
                do throws(Failure) {
                    try outcome.get()
                } catch {
                    raw.yield(.failure(error))
                }
                raw.finish()
            }
            raw.onTermination = { _ in task.cancel() }
            return stream
        }
    }
}

// MARK: - Continuation

public extension Publisher {
    /// The handle a custom ``Publisher/init(_:)`` body uses to emit values, finish, or fail.
    ///
    /// Mirrors `AsyncStream.Continuation`, but values are wrapped as `Result`: ``yield(_:)`` emits a
    /// success, ``fail(_:)`` emits a failure and seals the stream, and ``finish()`` completes it. Use
    /// `yieldAll` to emit a whole sequence, or ``suspendUntilCancelled()`` for callback-driven sources.
    struct Continuation: Sendable {
        private let _raw: AsyncStream<Result<Output, Failure>>.Continuation

        init(_ raw: AsyncStream<Result<Output, Failure>>.Continuation) {
            _raw = raw
        }

        /// Emits a value to subscribers.
        public func yield(_ value: Output) {
            _raw.yield(.success(value))
        }

        /// Emits a typed failure and seals the stream — no further values are delivered.
        public func fail(_ error: Failure) {
            _raw.yield(.failure(error))
            _raw.finish()
        }

        /// Completes the stream successfully.
        public func finish() {
            _raw.finish()
        }

        /// `true` once the `Task` is cancelled or the downstream unsubscribed. Check this in
        /// synchronous loops instead of `try Task.checkCancellation()`.
        public var isCancelled: Bool { Task.isCancelled }

        /// Emits every element of a synchronous sequence, stopping early if cancelled.
        public func yieldAll<S: Sequence>(_ sequence: S) where S.Element == Output {
            for value in sequence {
                guard !isCancelled else { return }
                yield(value)
            }
        }

        // Async sequence: cooperative cancellation ends the for-await automatically.
        // AsyncSequence.Failure is macOS 15+ / iOS 18+; gate accordingly.
        @available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, visionOS 2.0, *)
        public func yieldAll<S: AsyncSequence & Sendable>(_ sequence: S) async
        where S.Element == Output, S.Failure == Never {
            for await value in sequence {
                yield(value)
            }
        }

        // Park the body indefinitely. Use when the publisher is callback-driven
        // (e.g., delegate-based APIs). Returns when the Task is cancelled.
        public func suspendUntilCancelled() async {
            let (stream, cont) = AsyncStream<Void>.makeStream()
            await withTaskCancellationHandler {
                for await _ in stream {}
            } onCancel: {
                cont.finish()
            }
        }
    }
}

// MARK: - Type erasure

/// A type alias for ``Publisher``. Unlike Combine, `Publisher` is already a concrete, type-erased
/// value, so `AnyPublisher` exists only for source compatibility.
public typealias AnyPublisher<Output: Sendable, Failure: Error> = Publisher<Output, Failure>

public extension Publisher {
    /// Returns `self` — ``Publisher`` is already type-erased. Provided for Combine source compatibility.
    func eraseToAnyPublisher() -> Publisher<Output, Failure> { self }
}

// MARK: - Convenience constructors

public extension Publisher {
    /// A publisher that emits a single `value` and then finishes.
    static func just(_ value: Output) -> Publisher<Output, Failure> {
        Publisher { continuation in
            continuation.yield(value)
        }
    }

    /// A publisher that finishes immediately without emitting any value.
    static func empty() -> Publisher<Output, Failure> {
        Publisher { _ in }
    }
}

public extension Publisher where Failure == Never {
    /// A publisher that emits each element of `sequence` in order, then finishes.
    static func sequence<S: Sequence & Sendable>(_ sequence: S) -> Publisher<Output, Never>
    where S.Element == Output {
        Publisher { continuation in
            continuation.yieldAll(sequence)
        }
    }
}

public extension Publisher {
    static func fail(_ error: Failure) -> Publisher<Output, Failure> {
        Publisher { continuation in
            continuation.fail(error)
        }
    }
}
