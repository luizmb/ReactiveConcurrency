// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Stateful<s, DeferredStream<(a -> b)>> -> Stateful<s, DeferredStream<a>> -> Stateful<s, DeferredStream<b>>

/// Applicative apply for the Stateful-over-DeferredStream stack.
public func <*> <S, A: Sendable, B: Sendable>(
    _ sf: Stateful<S, DeferredStream<@Sendable (A) -> B>>,
    _ sa: Stateful<S, DeferredStream<A>>
) -> Stateful<S, DeferredStream<B>> {
    applyStatefulDeferredStream(sf, sa)
}

// (*>) :: Stateful<s, DeferredStream<a>> -> Stateful<s, DeferredStream<b>> -> Stateful<s, DeferredStream<b>>

/// Sequences two effects, keeping the right result for the Stateful-over-DeferredStream stack. Operator form of `seqRight`.
public func *> <S, A: Sendable, B: Sendable>(
    _ lhs: Stateful<S, DeferredStream<A>>,
    _ rhs: Stateful<S, DeferredStream<B>>
) -> Stateful<S, DeferredStream<B>> {
    seqRightStatefulDeferredStream(lhs, rhs)
}

// (<*) :: Stateful<s, DeferredStream<a>> -> Stateful<s, DeferredStream<b>> -> Stateful<s, DeferredStream<a>>

/// Sequences two effects, keeping the left result for the Stateful-over-DeferredStream stack. Operator form of `seqLeft`.
public func <* <S, A: Sendable, B: Sendable>(
    _ lhs: Stateful<S, DeferredStream<A>>,
    _ rhs: Stateful<S, DeferredStream<B>>
) -> Stateful<S, DeferredStream<A>> {
    seqLeftStatefulDeferredStream(lhs, rhs)
}
