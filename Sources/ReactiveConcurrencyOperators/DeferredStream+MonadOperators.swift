// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (>>-) :: DeferredStream a -> (a -> DeferredStream b) -> DeferredStream b
public func >>- <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<B>
) -> DeferredStream<B> {
    stream.flatMap(fn)
}

// (-<<) :: (a -> DeferredStream b) -> DeferredStream a -> DeferredStream b
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<B>,
    _ stream: DeferredStream<A>
) -> DeferredStream<B> {
    stream >>- fn
}

// (>=>) :: (a -> DeferredStream b) -> (b -> DeferredStream c) -> (a -> DeferredStream c)
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ f: @escaping @Sendable (A) -> DeferredStream<B>,
    _ g: @escaping @Sendable (B) -> DeferredStream<C>
) -> @Sendable (A) -> DeferredStream<C> {
    DeferredStream<A>.kleisli(f, g)
}

// (<=<) :: (b -> DeferredStream c) -> (a -> DeferredStream b) -> (a -> DeferredStream c)
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ g: @escaping @Sendable (B) -> DeferredStream<C>,
    _ f: @escaping @Sendable (A) -> DeferredStream<B>
) -> @Sendable (A) -> DeferredStream<C> {
    DeferredStream<B>.kleisliBack(g, f)
}
