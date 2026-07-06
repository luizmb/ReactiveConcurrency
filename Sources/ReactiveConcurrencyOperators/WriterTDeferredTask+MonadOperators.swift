// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Writer<w, DeferredTask<a>> -> (a -> Writer<w, DeferredTask<b>>) -> Writer<w, DeferredTask<b>>
public func >>- <W: Monoid, A: Sendable, B: Sendable>(
    _ writer: Writer<W, DeferredTask<A>>,
    _ fn: @escaping @Sendable (A) -> Writer<W, DeferredTask<B>>
) -> Writer<W, DeferredTask<B>> {
    writer.flatMapT(fn)
}

// (-<<) :: (a -> Writer<w, DeferredTask<b>>) -> Writer<w, DeferredTask<a>> -> Writer<w, DeferredTask<b>>
public func -<< <W: Monoid, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> Writer<W, DeferredTask<B>>,
    _ writer: Writer<W, DeferredTask<A>>
) -> Writer<W, DeferredTask<B>> {
    writer.flatMapT(fn)
}

// (>=>) :: (a -> Writer<w, DeferredTask<b>>) -> (b -> Writer<w, DeferredTask<c>>) -> a -> Writer<w, DeferredTask<c>>
public func >=> <W: Monoid, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> Writer<W, DeferredTask<B>>,
    _ fn2: @escaping @Sendable (B) -> Writer<W, DeferredTask<C>>
) -> @Sendable (A) -> Writer<W, DeferredTask<C>> {
    { a in fn1(a).flatMapT(fn2) }
}
