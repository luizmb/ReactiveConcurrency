// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Writer<w, Publisher<a, f>> -> (a -> Writer<w, Publisher<b, f>>) -> Writer<w, Publisher<b, f>>
public func >>- <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ writer: Writer<W, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> Writer<W, Publisher<B, F>>
) -> Writer<W, Publisher<B, F>> {
    writer.flatMapT(fn)
}

// (-<<) :: (a -> Writer<w, Publisher<b, f>>) -> Writer<w, Publisher<a, f>> -> Writer<w, Publisher<b, f>>
public func -<< <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Writer<W, Publisher<B, F>>,
    _ writer: Writer<W, Publisher<A, F>>
) -> Writer<W, Publisher<B, F>> {
    writer.flatMapT(fn)
}

// (>=>) :: (a -> Writer<w, Publisher<b, f>>) -> (b -> Writer<w, Publisher<c, f>>) -> a -> Writer<w, Publisher<c, f>>
public func >=> <W: Monoid, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Writer<W, Publisher<B, F>>,
    _ fn2: @escaping @Sendable (B) -> Writer<W, Publisher<C, F>>
) -> @Sendable (A) -> Writer<W, Publisher<C, F>> {
    { a in fn1(a).flatMapT(fn2) }
}
