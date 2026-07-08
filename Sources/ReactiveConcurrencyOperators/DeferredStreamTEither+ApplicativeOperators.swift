// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<Either<l,a->b>> -> DeferredStream<Either<l,a>> -> DeferredStream<Either<l,b>>

/// Applicative apply for the DeferredStream-over-Either stack.
public func <*> <L: Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredStream<Either<L, @Sendable (A) -> B>>,
    _ values: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    applyTDeferredStreamEither(fns, values)
}
