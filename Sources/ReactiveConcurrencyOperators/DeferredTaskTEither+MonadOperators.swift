// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<Either<l,a>> -> (a -> DeferredTask<Either<l,b>>) -> DeferredTask<Either<l,b>>
public func >>- <L: Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> DeferredTask<Either<L, B>> {
    flatMapTDeferredTaskEither(task, fn)
}

// (-<<) :: (a -> DeferredTask<Either<l,b>>) -> DeferredTask<Either<l,a>> -> DeferredTask<Either<l,b>>
public func -<< <L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>,
    _ task: DeferredTask<Either<L, A>>
) -> DeferredTask<Either<L, B>> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask<Either<l,b>>) -> (b -> DeferredTask<Either<l,c>>) -> a -> DeferredTask<Either<l,c>>
public func >=> <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Either<L, C>>
) -> @Sendable (A) -> DeferredTask<Either<L, C>> {
    kleisliTDeferredTaskEither(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<Either<l,c>>) -> (a -> DeferredTask<Either<l,b>>) -> a -> DeferredTask<Either<l,c>>
public func <=< <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Either<L, C>>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> @Sendable (A) -> DeferredTask<Either<L, C>> {
    fn1 >=> fn2
}
