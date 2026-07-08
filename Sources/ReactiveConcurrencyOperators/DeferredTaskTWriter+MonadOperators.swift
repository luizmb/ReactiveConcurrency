// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<Writer<w, a>> -> (a -> DeferredTask<Writer<w, b>>) -> DeferredTask<Writer<w, b>>
public func >>- <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Writer<W, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, B>> {
    task.flatMapT(fn)
}

// (-<<) :: (a -> DeferredTask<Writer<w, b>>) -> DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>>
public func -<< <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>,
    _ task: DeferredTask<Writer<W, A>>
) -> DeferredTask<Writer<W, B>> {
    task.flatMapT(fn)
}

// (>=>) :: (a -> DeferredTask<Writer<w, b>>) -> (b -> DeferredTask<Writer<w, c>>) -> a -> DeferredTask<Writer<w, c>>
public func >=> <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Writer<W, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Writer<W, C>>
) -> @Sendable (A) -> DeferredTask<Writer<W, C>> {
    { a in fn1(a).flatMapT(fn2) }
}
