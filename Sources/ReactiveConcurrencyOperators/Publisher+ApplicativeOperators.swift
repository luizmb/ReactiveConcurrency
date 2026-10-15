// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<*>) :: Publisher (a -> b) e -> Publisher a e -> Publisher b e
// `ap`, derived from the concat bind: the cartesian product in order. Positional pairing is `zip`.

/// Applicative apply (`ap`): for each function, in order, every value (cartesian, agrees with `>>-`).
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
