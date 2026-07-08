// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredTask<Result<a->b,e>> -> DeferredTask<Result<a,e>> -> DeferredTask<Result<b,e>>

/// Applicative apply for the DeferredTask-over-Result stack.
public func <*> <A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fns: DeferredTask<Result<@Sendable (A) -> B, E>>,
    _ values: DeferredTask<Result<A, E>>
) -> DeferredTask<Result<B, E>> {
    applyTDeferredTaskResult(fns, values)
}
