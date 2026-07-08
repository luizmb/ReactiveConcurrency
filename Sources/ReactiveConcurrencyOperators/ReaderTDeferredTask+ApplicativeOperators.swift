// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Reader<env, DeferredTask<(a -> b)>> -> Reader<env, DeferredTask<a>> -> Reader<env, DeferredTask<b>>

/// Applicative apply for the Reader-over-DeferredTask stack.
public func <*> <Env, A: Sendable, B: Sendable>(
    _ rf: Reader<Env, DeferredTask<@Sendable (A) -> B>>,
    _ ra: Reader<Env, DeferredTask<A>>
) -> Reader<Env, DeferredTask<B>> {
    applyReaderDeferredTask(rf, ra)
}

// (*>) :: Reader<env, DeferredTask<a>> -> Reader<env, DeferredTask<b>> -> Reader<env, DeferredTask<b>>

/// Sequences two effects, keeping the right result for the Reader-over-DeferredTask stack. Operator form of `seqRight`.
public func *> <Env, A: Sendable, B: Sendable>(
    _ lhs: Reader<Env, DeferredTask<A>>,
    _ rhs: Reader<Env, DeferredTask<B>>
) -> Reader<Env, DeferredTask<B>> {
    seqRightReaderDeferredTask(lhs, rhs)
}

// (<*) :: Reader<env, DeferredTask<a>> -> Reader<env, DeferredTask<b>> -> Reader<env, DeferredTask<a>>

/// Sequences two effects, keeping the left result for the Reader-over-DeferredTask stack. Operator form of `seqLeft`.
public func <* <Env, A: Sendable, B: Sendable>(
    _ lhs: Reader<Env, DeferredTask<A>>,
    _ rhs: Reader<Env, DeferredTask<B>>
) -> Reader<Env, DeferredTask<A>> {
    seqLeftReaderDeferredTask(lhs, rhs)
}
