// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<|>) :: DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>>
public func <|> <A: Sendable, E: Error & Sendable>(
    _ lhs: DeferredTask<Result<A, E>>,
    _ rhs: @autoclosure () -> DeferredTask<Result<A, E>>
) -> DeferredTask<Result<A, E>> {
    altDeferredTaskResult(lhs, rhs())
}
