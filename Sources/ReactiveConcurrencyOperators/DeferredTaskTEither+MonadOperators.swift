// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<Either<l,a>> -> (a -> DeferredTask<Either<l,b>>) -> DeferredTask<Either<l,b>>

/// Monadic bind — sequences a dependent effect (container on the left) for the DeferredTask-over-Either stack. Operator form of `flatMap`.
public func >>- <L: Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> DeferredTask<Either<L, B>> {
    flatMapTDeferredTaskEither(task, fn)
}

// (-<<) :: (a -> DeferredTask<Either<l,b>>) -> DeferredTask<Either<l,a>> -> DeferredTask<Either<l,b>>

/// Monadic bind — sequences a dependent effect (function on the left) for the DeferredTask-over-Either stack. Operator form of `flatMap`.
public func -<< <L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>,
    _ task: DeferredTask<Either<L, A>>
) -> DeferredTask<Either<L, B>> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask<Either<l,b>>) -> (b -> DeferredTask<Either<l,c>>) -> a -> DeferredTask<Either<l,c>>

/// Left-to-right Kleisli composition of two effectful functions for the DeferredTask-over-Either stack.
public func >=> <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Either<L, C>>
) -> @Sendable (A) -> DeferredTask<Either<L, C>> {
    kleisliTDeferredTaskEither(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<Either<l,c>>) -> (a -> DeferredTask<Either<l,b>>) -> a -> DeferredTask<Either<l,c>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the DeferredTask-over-Either stack.
public func <=< <L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Either<L, C>>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> @Sendable (A) -> DeferredTask<Either<L, C>> {
    fn1 >=> fn2
}
