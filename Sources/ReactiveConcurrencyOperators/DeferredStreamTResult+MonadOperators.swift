// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredStream<Result<a,e>> -> (a -> DeferredStream<Result<b,e>>) -> DeferredStream<Result<b,e>>
public func >>- <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ stream: DeferredStream<Result<A, E>>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<Result<B, E>>
) -> DeferredStream<Result<B, E>> {
    flatMapTDeferredStreamResult(stream, fn)
}

// (-<<) :: (a -> DeferredStream<Result<b,e>>) -> DeferredStream<Result<a,e>> -> DeferredStream<Result<b,e>>
public func -<< <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<Result<B, E>>,
    _ stream: DeferredStream<Result<A, E>>
) -> DeferredStream<Result<B, E>> {
    stream >>- fn
}

// (>=>) :: (a -> DeferredStream<Result<b,e>>) -> (b -> DeferredStream<Result<c,e>>) -> a -> DeferredStream<Result<c,e>>
public func >=> <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Result<B, E>>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Result<C, E>>
) -> @Sendable (A) -> DeferredStream<Result<C, E>> {
    kleisliTDeferredStreamResult(fn1, fn2)
}

// (<=<) :: (b -> DeferredStream<Result<c,e>>) -> (a -> DeferredStream<Result<b,e>>) -> a -> DeferredStream<Result<c,e>>
public func <=< <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Result<C, E>>,
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Result<B, E>>
) -> @Sendable (A) -> DeferredStream<Result<C, E>> {
    fn1 >=> fn2
}
