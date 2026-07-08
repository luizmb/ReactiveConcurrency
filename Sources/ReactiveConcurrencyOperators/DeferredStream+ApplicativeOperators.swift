// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<*>) :: DeferredStream (a -> b) -> DeferredStream a -> DeferredStream b
// Zippy Semigroupal (ZipList-style), NOT the cartesian monad-derived applicative: pairs
// positionally and truncates at the shorter side. See DeferredStream+Applicative.swift for the caveat.

/// Applicative apply — zippy (positional) combination of wrapped functions and values.
public func <*> <A: Sendable, B: Sendable>(
    _ fns: DeferredStream<@Sendable (A) -> B>,
    _ values: DeferredStream<A>
) -> DeferredStream<B> {
    applyDeferredStream(fns, values)
}

// (*>) :: DeferredStream a -> DeferredStream b -> DeferredStream b

/// Sequences two effects, keeping the right result. Operator form of `seqRight`.
public func *> <A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<A>,
    _ rhs: DeferredStream<B>
) -> DeferredStream<B> {
    lhs.seqRight(rhs)
}

// (<*) :: DeferredStream a -> DeferredStream b -> DeferredStream a

/// Sequences two effects, keeping the left result. Operator form of `seqLeft`.
public func <* <A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<A>,
    _ rhs: DeferredStream<B>
) -> DeferredStream<A> {
    lhs.seqLeft(rhs)
}
