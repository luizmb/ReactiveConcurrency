// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Stateful<s, Publisher<a->b, f>> -> Stateful<s, Publisher<a, f>> -> Stateful<s, Publisher<b, f>>
public func <*> <S, A: Sendable, B: Sendable, F: Error>(
    _ sf: Stateful<S, Publisher<@Sendable (A) -> B, F>>,
    _ sa: Stateful<S, Publisher<A, F>>
) -> Stateful<S, Publisher<B, F>> {
    applyStatefulPublisher(sf, sa)
}

// (*>) :: Stateful<s, Publisher<a, f>> -> Stateful<s, Publisher<b, f>> -> Stateful<s, Publisher<b, f>>
public func *> <S, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Stateful<S, Publisher<A, F>>,
    _ rhs: Stateful<S, Publisher<B, F>>
) -> Stateful<S, Publisher<B, F>> {
    seqRightStatefulPublisher(lhs, rhs)
}

// (<*) :: Stateful<s, Publisher<a, f>> -> Stateful<s, Publisher<b, f>> -> Stateful<s, Publisher<a, f>>
public func <* <S, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Stateful<S, Publisher<A, F>>,
    _ rhs: Stateful<S, Publisher<B, F>>
) -> Stateful<S, Publisher<A, F>> {
    seqLeftStatefulPublisher(lhs, rhs)
}
