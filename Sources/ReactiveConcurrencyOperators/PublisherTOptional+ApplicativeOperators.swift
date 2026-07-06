// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Publisher<(a->b)?, f> -> Publisher<a?, f> -> Publisher<b?, f>
public func <*> <A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<(@Sendable (A) -> B)?, F>,
    _ values: Publisher<A?, F>
) -> Publisher<B?, F> {
    applyTPublisherOptional(fns, values)
}
