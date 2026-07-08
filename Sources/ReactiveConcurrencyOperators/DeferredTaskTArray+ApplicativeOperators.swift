// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredTask<[a->b]> -> DeferredTask<[a]> -> DeferredTask<[b]>

/// Applicative apply for the DeferredTask-over-Array stack.
public func <*> <A: Sendable, B: Sendable>(
    _ fns: DeferredTask<[@Sendable (A) -> B]>,
    _ values: DeferredTask<[A]>
) -> DeferredTask<[B]> {
    applyTDeferredTaskArray(fns, values)
}
