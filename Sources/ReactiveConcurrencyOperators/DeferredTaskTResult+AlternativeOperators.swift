// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrencyTransformers

// (<|>) :: DeferredTaskTResult a -> DeferredTaskTResult a -> DeferredTaskTResult a

/// Left-biased choice for the ``DeferredTaskTResult`` stack: operator form of `alt`.
public func <|> <E: Error, A: Sendable>(
    _ lhs: ReactiveConcurrencyTransformers.DeferredTaskTResult<E, A>,
    _ rhs: @autoclosure () -> ReactiveConcurrencyTransformers.DeferredTaskTResult<E, A>
) -> ReactiveConcurrencyTransformers.DeferredTaskTResult<E, A> {
    lhs.alt(rhs())
}
