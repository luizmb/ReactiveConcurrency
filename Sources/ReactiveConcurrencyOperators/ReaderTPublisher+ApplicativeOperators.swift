// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Reader<env, Publisher<a->b, f>> -> Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>>

/// Applicative apply for the Reader-over-Publisher stack.
public func <*> <Env, A: Sendable, B: Sendable, F: Error>(
    _ rf: Reader<Env, Publisher<@Sendable (A) -> B, F>>,
    _ ra: Reader<Env, Publisher<A, F>>
) -> Reader<Env, Publisher<B, F>> {
    applyReaderPublisher(rf, ra)
}

// (*>) :: Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>> -> Reader<env, Publisher<b, f>>

/// Sequences two effects, keeping the right result for the Reader-over-Publisher stack. Operator form of `seqRight`.
public func *> <Env, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Reader<Env, Publisher<A, F>>,
    _ rhs: Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    seqRightReaderPublisher(lhs, rhs)
}

// (<*) :: Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>> -> Reader<env, Publisher<a, f>>

/// Sequences two effects, keeping the left result for the Reader-over-Publisher stack. Operator form of `seqLeft`.
public func <* <Env, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Reader<Env, Publisher<A, F>>,
    _ rhs: Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<A, F>> {
    seqLeftReaderPublisher(lhs, rhs)
}
