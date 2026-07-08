// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredStream<a?> -> (a -> DeferredStream<b?>) -> DeferredStream<b?>
public func >>- <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A?>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<B?>
) -> DeferredStream<B?> {
    flatMapTDeferredStreamOptional(stream, fn)
}

// (-<<) :: (a -> DeferredStream<b?>) -> DeferredStream<a?> -> DeferredStream<b?>
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<B?>,
    _ stream: DeferredStream<A?>
) -> DeferredStream<B?> {
    stream >>- fn
}

// (>=>) :: (a -> DeferredStream<b?>) -> (b -> DeferredStream<c?>) -> a -> DeferredStream<c?>
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<B?>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<C?>
) -> @Sendable (A) -> DeferredStream<C?> {
    kleisliTDeferredStreamOptional(fn1, fn2)
}

// (<=<) :: (b -> DeferredStream<c?>) -> (a -> DeferredStream<b?>) -> a -> DeferredStream<c?>
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredStream<C?>,
    _ fn1: @escaping @Sendable (A) -> DeferredStream<B?>
) -> @Sendable (A) -> DeferredStream<C?> {
    fn1 >=> fn2
}
