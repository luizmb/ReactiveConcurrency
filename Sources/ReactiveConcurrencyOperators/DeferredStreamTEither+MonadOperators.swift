// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredStream<Either<l,a>> -> (a -> DeferredStream<Either<l,b>>) -> DeferredStream<Either<l,b>>
public func >>- <L: Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>
) -> DeferredStream<Either<L, B>> {
    flatMapTDeferredStreamEither(stream, fn)
}

// (-<<) :: (a -> DeferredStream<Either<l,b>>) -> DeferredStream<Either<l,a>> -> DeferredStream<Either<l,b>>
public func -<< <L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>,
    _ stream: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    stream >>- fn
}

// (>=>) :: (a -> DeferredStream<Either<l,b>>) -> (b -> DeferredStream<Either<l,c>>) -> a -> DeferredStream<Either<l,c>>
public func >=> <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Either<L, C>>
) -> @Sendable (A) -> DeferredStream<Either<L, C>> {
    kleisliTDeferredStreamEither(fn1, fn2)
}

// (<=<) :: (b -> DeferredStream<Either<l,c>>) -> (a -> DeferredStream<Either<l,b>>) -> a -> DeferredStream<Either<l,c>>
public func <=< <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Either<L, C>>,
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>
) -> @Sendable (A) -> DeferredStream<Either<L, C>> {
    fn1 >=> fn2
}
