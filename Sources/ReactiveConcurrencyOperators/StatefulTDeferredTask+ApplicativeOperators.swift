// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Stateful<s, DeferredTask<(a -> b)>> -> Stateful<s, DeferredTask<a>> -> Stateful<s, DeferredTask<b>>

/// Applicative apply for the Stateful-over-DeferredTask stack.
public func <*> <S, A: Sendable, B: Sendable>(
    _ sf: Stateful<S, DeferredTask<@Sendable (A) -> B>>,
    _ sa: Stateful<S, DeferredTask<A>>
) -> Stateful<S, DeferredTask<B>> {
    applyStatefulDeferredTask(sf, sa)
}

// (*>) :: Stateful<s, DeferredTask<a>> -> Stateful<s, DeferredTask<b>> -> Stateful<s, DeferredTask<b>>

/// Sequences two effects, keeping the right result for the Stateful-over-DeferredTask stack. Operator form of `seqRight`.
public func *> <S, A: Sendable, B: Sendable>(
    _ lhs: Stateful<S, DeferredTask<A>>,
    _ rhs: Stateful<S, DeferredTask<B>>
) -> Stateful<S, DeferredTask<B>> {
    seqRightStatefulDeferredTask(lhs, rhs)
}

// (<*) :: Stateful<s, DeferredTask<a>> -> Stateful<s, DeferredTask<b>> -> Stateful<s, DeferredTask<a>>

/// Sequences two effects, keeping the left result for the Stateful-over-DeferredTask stack. Operator form of `seqLeft`.
public func <* <S, A: Sendable, B: Sendable>(
    _ lhs: Stateful<S, DeferredTask<A>>,
    _ rhs: Stateful<S, DeferredTask<B>>
) -> Stateful<S, DeferredTask<A>> {
    seqLeftStatefulDeferredTask(lhs, rhs)
}
