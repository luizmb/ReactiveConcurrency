// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<*>) :: DeferredStream (a -> b) -> DeferredStream a -> DeferredStream b
// `ap`, derived from the concat bind: the cartesian product in order. Positional pairing is `zip`.

/// Applicative apply (`ap`): for each function, in order, every value (cartesian, agrees with `>>-`).
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
