// SPDX-License-Identifier: Apache-2.0

public extension DeferredStream {
    /// Lifts a single value into a one-element stream (applicative `pure`).
    static func pure(_ value: Element) -> DeferredStream<Element> {
        DeferredStream { AsyncStream { continuation in
            continuation.yield(value)
            continuation.finish()
        }
        }
    }

    /// Zips positionally with `rhs`, keeping only `rhs`'s elements; truncates to the shorter stream.
    func seqRight<B: Sendable>(_ rhs: DeferredStream<B>) -> DeferredStream<B> {
        liftA2DeferredStream { _, b in b }(self, rhs)
    }

    /// Zips positionally with `rhs`, keeping only `self`'s elements; truncates to the shorter stream.
    func seqLeft<B: Sendable>(_ rhs: DeferredStream<B>) -> DeferredStream<Element> {
        liftA2DeferredStream { a, _ in a }(self, rhs)
    }

    /// Pairs elements of two streams positionally, stopping when either stream ends.
    static func zip<B: Sendable>(
        _ sa: DeferredStream<Element>,
        _ sb: DeferredStream<B>
    ) -> DeferredStream<(Element, B)> {
        DeferredStream<(Element, B)> {
            let streamA = sa.factory()
            let streamB = sb.factory()
            return AsyncStream<(Element, B)> { continuation in
                let task = Task { @Sendable in
                    var ia = streamA.makeAsyncIterator()
                    var ib = streamB.makeAsyncIterator()
                    while let a = await ia.next(), let b = await ib.next() {
                        continuation.yield((a, b))
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// Pairs elements of three streams positionally, stopping when any stream ends.
    static func zip3<B: Sendable, C: Sendable>(
        _ sa: DeferredStream<Element>,
        _ sb: DeferredStream<B>,
        _ sc: DeferredStream<C>
    ) -> DeferredStream<(Element, B, C)> {
        DeferredStream<(Element, B, C)> {
            let streamA = sa.factory()
            let streamB = sb.factory()
            let streamC = sc.factory()
            return AsyncStream<(Element, B, C)> { continuation in
                let task = Task { @Sendable in
                    var ia = streamA.makeAsyncIterator()
                    var ib = streamB.makeAsyncIterator()
                    var ic = streamC.makeAsyncIterator()
                    while let a = await ia.next(), let b = await ib.next(), let c = await ic.next() {
                        continuation.yield((a, b, c))
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// Pairs elements of four streams positionally, stopping when any stream ends.
    static func zip4<B: Sendable, C: Sendable, D: Sendable>(
        _ sa: DeferredStream<Element>,
        _ sb: DeferredStream<B>,
        _ sc: DeferredStream<C>,
        _ sd: DeferredStream<D>
    ) -> DeferredStream<(Element, B, C, D)> {
        DeferredStream<(Element, B, C, D)> {
            let streamA = sa.factory()
            let streamB = sb.factory()
            let streamC = sc.factory()
            let streamD = sd.factory()
            return AsyncStream<(Element, B, C, D)> { continuation in
                let task = Task { @Sendable in
                    var ia = streamA.makeAsyncIterator()
                    var ib = streamB.makeAsyncIterator()
                    var ic = streamC.makeAsyncIterator()
                    var id = streamD.makeAsyncIterator()
                    while let a = await ia.next(), let b = await ib.next(),
                          let c = await ic.next(), let d = await id.next() {
                        continuation.yield((a, b, c, d))
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }
}

///
/// Zippy Semigroupal (ZipList-style): pairs each fn with each value positionally and truncates at
/// the shorter side. This is the product users want from a stream, but it is NOT the applicative
/// derived from the monad (`flatMap` = concatMap = cartesian). The applicative
/// *identity* law `pure(id) <*> v == v` fails for `|v| > 1` (pure yields one element, zip truncates
/// `v` to length 1). Use `flatMap` for the cartesian, monad-consistent product.
/// `pure` / `seqLeft` / `seqRight` are zippy for the same reason.

/// Applies a stream of functions to a stream of values, positionally (applicative `<*>`).
public func applyDeferredStream<A: Sendable, B: Sendable>(
    _ fns: DeferredStream<@Sendable (A) -> B>,
    _ values: DeferredStream<A>
) -> DeferredStream<B> {
    DeferredStream<B> {
        let fnStream = fns.factory()
        let valStream = values.factory()
        return AsyncStream<B> { continuation in
            let task = Task { @Sendable in
                var fnIter = fnStream.makeAsyncIterator()
                var valIter = valStream.makeAsyncIterator()
                while let fn = await fnIter.next(), let val = await valIter.next() {
                    continuation.yield(fn(val))
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

/// Combines two streams element-wise with a binary function; truncates to the shorter stream.
public func liftA2DeferredStream<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<A>, DeferredStream<B>) -> DeferredStream<C> {
    { @Sendable sa, sb in
        DeferredStream<C> {
            let streamA = sa.factory()
            let streamB = sb.factory()
            return AsyncStream<C> { continuation in
                let task = Task { @Sendable in
                    var ia = streamA.makeAsyncIterator()
                    var ib = streamB.makeAsyncIterator()
                    while let a = await ia.next(), let b = await ib.next() {
                        continuation.yield(fn(a, b))
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }
}
