// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrencyTransformers

// (<|>) :: DeferredTaskTOptional a -> DeferredTaskTOptional a -> DeferredTaskTOptional a

/// Left-biased choice for the ``DeferredTaskTOptional`` stack: operator form of `alt`.
public func <|> <A: Sendable>(
    _ lhs: ReactiveConcurrencyTransformers.DeferredTaskTOptional<A>,
    _ rhs: @autoclosure () -> ReactiveConcurrencyTransformers.DeferredTaskTOptional<A>
) -> ReactiveConcurrencyTransformers.DeferredTaskTOptional<A> {
    lhs.alt(rhs())
}
