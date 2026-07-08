// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<*>) :: Publisher (a -> b) e -> Publisher a e -> Publisher b e
// Zippy Semigroupal (ZipList-style), NOT the cartesian monad-derived applicative: pairs
// positionally and truncates at the shorter side. See Publisher+FP.swift for the law caveat.

/// Applicative apply — zippy (positional) combination of wrapped functions and values.
public func <*> <A: Sendable, B: Sendable, E: Error>(
    _ fns: Publisher<@Sendable (A) -> B, E>,
    _ values: Publisher<A, E>
) -> Publisher<B, E> {
    applyPublisher(fns, values)
}

// (*>) :: Publisher a e -> Publisher b e -> Publisher b e

/// Sequences two effects, keeping the right result. Operator form of `seqRight`.
public func *> <A: Sendable, B: Sendable, E: Error>(
    _ lhs: Publisher<A, E>,
    _ rhs: Publisher<B, E>
) -> Publisher<B, E> {
    lhs.seqRight(rhs)
}

// (<*) :: Publisher a e -> Publisher b e -> Publisher a e

/// Sequences two effects, keeping the left result. Operator form of `seqLeft`.
public func <* <A: Sendable, B: Sendable, E: Error>(
    _ lhs: Publisher<A, E>,
    _ rhs: Publisher<B, E>
) -> Publisher<A, E> {
    lhs.seqLeft(rhs)
}
