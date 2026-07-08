// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredStream<Writer<w, a>> -> (a -> DeferredStream<Writer<w, b>>) -> DeferredStream<Writer<w, b>>
public func >>- <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Writer<W, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, B>> {
    stream.flatMapT(fn)
}

// (-<<) :: (a -> DeferredStream<Writer<w, b>>) -> DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>>
public func -<< <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<Writer<W, B>>,
    _ stream: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    stream.flatMapT(fn)
}

// (>=>) :: (a -> DeferredStream<Writer<w, b>>) -> (b -> DeferredStream<Writer<w, c>>) -> a -> DeferredStream<Writer<w, c>>
public func >=> <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Writer<W, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Writer<W, C>>
) -> @Sendable (A) -> DeferredStream<Writer<W, C>> {
    { a in fn1(a).flatMapT(fn2) }
}
