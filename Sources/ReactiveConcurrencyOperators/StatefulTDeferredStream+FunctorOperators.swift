// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Stateful<s, DeferredStream<a>> -> Stateful<s, DeferredStream<b>>

/// Functor map lifted through the transformer (function on the left) for the Stateful-over-DeferredStream stack. Operator form of `mapT`.
public func <£^> <S, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stateful: Stateful<S, DeferredStream<A>>
) -> Stateful<S, DeferredStream<B>> {
    stateful.mapT(fn)
}

// (<&^>) :: Stateful<s, DeferredStream<a>> -> (a -> b) -> Stateful<s, DeferredStream<b>>

/// Functor map lifted through the transformer (container on the left) for the Stateful-over-DeferredStream stack. Operator form of `mapT`.
public func <&^> <S, A: Sendable, B: Sendable>(
    _ stateful: Stateful<S, DeferredStream<A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Stateful<S, DeferredStream<B>> {
    stateful.mapT(fn)
}
