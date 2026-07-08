// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Either<l,a>, f> -> (a -> Publisher<Either<l,b>, f>) -> Publisher<Either<l,b>, f>

/// Monadic bind — sequences a dependent effect (container on the left) for the Publisher-over-Either stack. Operator form of `flatMap`.
public func >>- <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Either<L, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>
) -> Publisher<Either<L, B>, F> {
    flatMapTPublisherEither(publisher, fn)
}

// (-<<) :: (a -> Publisher<Either<l,b>, f>) -> Publisher<Either<l,a>, f> -> Publisher<Either<l,b>, f>

/// Monadic bind — sequences a dependent effect (function on the left) for the Publisher-over-Either stack. Operator form of `flatMap`.
public func -<< <L: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>,
    _ publisher: Publisher<Either<L, A>, F>
) -> Publisher<Either<L, B>, F> {
    publisher >>- fn
}

// (>=>) :: (a -> Publisher<Either<l,b>, f>) -> (b -> Publisher<Either<l,c>, f>) -> a -> Publisher<Either<l,c>, f>

/// Left-to-right Kleisli composition of two effectful functions for the Publisher-over-Either stack.
public func >=> <L: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<Either<L, C>, F>
) -> @Sendable (A) -> Publisher<Either<L, C>, F> {
    kleisliTPublisherEither(fn1, fn2)
}

// (<=<) :: (b -> Publisher<Either<l,c>, f>) -> (a -> Publisher<Either<l,b>, f>) -> a -> Publisher<Either<l,c>, f>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the Publisher-over-Either stack.
public func <=< <L: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn2: @escaping @Sendable (B) -> Publisher<Either<L, C>, F>,
    _ fn1: @escaping @Sendable (A) -> Publisher<Either<L, B>, F>
) -> @Sendable (A) -> Publisher<Either<L, C>, F> {
    fn1 >=> fn2
}
