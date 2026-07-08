// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<(a->b)?> -> DeferredStream<a?> -> DeferredStream<b?>

/// Applicative apply for the DeferredStream-over-Optional stack.
public func <*> <A: Sendable, B: Sendable>(
    _ fns: DeferredStream<(@Sendable (A) -> B)?>,
    _ values: DeferredStream<A?>
) -> DeferredStream<B?> {
    applyTDeferredStreamOptional(fns, values)
}
