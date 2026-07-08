// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredTask<Either<l,a->b>> -> DeferredTask<Either<l,a>> -> DeferredTask<Either<l,b>>

/// Applicative apply for the DeferredTask-over-Either stack.
public func <*> <L: Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredTask<Either<L, @Sendable (A) -> B>>,
    _ values: DeferredTask<Either<L, A>>
) -> DeferredTask<Either<L, B>> {
    applyTDeferredTaskEither(fns, values)
}
