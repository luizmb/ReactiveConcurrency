// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredStream<Either<l,a>> -> DeferredStream<Either<l,b>>
public func <£^> <L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    mapTDeferredStreamEither(fn, stream)
}

// (<&^>) :: DeferredStream<Either<l,a>> -> (a -> b) -> DeferredStream<Either<l,b>>
public func <&^> <L: Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<Either<L, B>> {
    mapTDeferredStreamEither(fn, stream)
}
