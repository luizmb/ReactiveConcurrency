// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Reader<env, DeferredStream<(a -> b)>> -> Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>>

/// Applicative apply for the Reader-over-DeferredStream stack.
public func <*> <Env, A: Sendable, B: Sendable>(
    _ rf: Reader<Env, DeferredStream<@Sendable (A) -> B>>,
    _ ra: Reader<Env, DeferredStream<A>>
) -> Reader<Env, DeferredStream<B>> {
    applyReaderDeferredStream(rf, ra)
}

// (*>) :: Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>> -> Reader<env, DeferredStream<b>>

/// Sequences two effects, keeping the right result for the Reader-over-DeferredStream stack. Operator form of `seqRight`.
public func *> <Env, A: Sendable, B: Sendable>(
    _ lhs: Reader<Env, DeferredStream<A>>,
    _ rhs: Reader<Env, DeferredStream<B>>
) -> Reader<Env, DeferredStream<B>> {
    seqRightReaderDeferredStream(lhs, rhs)
}

// (<*) :: Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>> -> Reader<env, DeferredStream<a>>

/// Sequences two effects, keeping the left result for the Reader-over-DeferredStream stack. Operator form of `seqLeft`.
public func <* <Env, A: Sendable, B: Sendable>(
    _ lhs: Reader<Env, DeferredStream<A>>,
    _ rhs: Reader<Env, DeferredStream<B>>
) -> Reader<Env, DeferredStream<A>> {
    seqLeftReaderDeferredStream(lhs, rhs)
}
