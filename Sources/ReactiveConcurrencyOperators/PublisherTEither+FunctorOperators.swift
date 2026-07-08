// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Publisher<Either<l,a>, f> -> Publisher<Either<l,b>, f>

/// Functor map lifted through the transformer (function on the left) for the Publisher-over-Either stack. Operator form of `mapT`.
public func <£^> <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    mapTPublisherEither(fn, publisher)
}

// (<&^>) :: Publisher<Either<l,a>, f> -> (a -> b) -> Publisher<Either<l,b>, f>

/// Functor map lifted through the transformer (container on the left) for the Publisher-over-Either stack. Operator form of `mapT`.
public func <&^> <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Either<L, A>, F>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<Either<L, B>, F> {
    mapTPublisherEither(fn, publisher)
}
