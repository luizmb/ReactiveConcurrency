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

    /// Sequences with `rhs`, keeping `rhs`'s elements (`*>`): for each element of `self`, every element
    /// of `rhs` (the cartesian product, derived from `flatMap`). Use `zip` for positional pairing.
    func seqRight<B: Sendable>(_ rhs: DeferredStream<B>) -> DeferredStream<B> {
        liftA2DeferredStream { _, b in b }(self, rhs)
    }

    /// Sequences with `rhs`, keeping `self`'s elements (`<*`): each element of `self` repeated once per
    /// element of `rhs` (the cartesian product, derived from `flatMap`). Use `zip` for positional pairing.
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

/// Applies a stream of functions to a stream of values (applicative `<*>`).
///
/// `ap`, derived from the monad: for each function, in order, every value (`fns.flatMap { f in
/// values.map(f) }`), so `<*>` agrees with `>>-` and the applicative laws hold. `values` is
/// re-subscribed once per function. For positional, ZipList-style pairing use `zip`.
public func applyDeferredStream<A: Sendable, B: Sendable>(
    _ fns: DeferredStream<@Sendable (A) -> B>,
    _ values: DeferredStream<A>
) -> DeferredStream<B> {
    fns.flatMap { fn in values.map(fn) }
}

/// Combines two streams with a binary function: for each element of `sa`, in order, every element of
/// `sb` (the cartesian product, derived from `flatMap`). For positional pairing use `zip`.
public func liftA2DeferredStream<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<A>, DeferredStream<B>) -> DeferredStream<C> {
    { @Sendable sa, sb in
        sa.flatMap { a in sb.map { b in fn(a, b) } }
    }
}
