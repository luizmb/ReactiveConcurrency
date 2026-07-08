// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<|>) :: DeferredStream a -> DeferredStream a -> DeferredStream a

/// Alternative — first non-empty/successful of the two, a lawful monoid with `empty`.
public func <|> <A: Sendable>(_ lhs: DeferredStream<A>, _ rhs: @autoclosure () -> DeferredStream<A>) -> DeferredStream<A> {
    DeferredStream.alt(lhs, rhs())
}
