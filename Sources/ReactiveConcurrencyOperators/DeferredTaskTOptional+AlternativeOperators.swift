// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<|>) :: DeferredTask<A?> -> DeferredTask<A?> -> DeferredTask<A?>

/// Alternative — first non-empty/successful of the two, a lawful monoid with `empty` for the DeferredTask-over-Optional stack.
public func <|> <A: Sendable>(
    _ lhs: DeferredTask<A?>,
    _ rhs: @autoclosure () -> DeferredTask<A?>
) -> DeferredTask<A?> {
    altDeferredTaskOptional(lhs, rhs())
}
