// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Stateful<s, Publisher<a, f>> -> Stateful<s, Publisher<b, f>>

/// Functor map lifted through the transformer (function on the left) for the Stateful-over-Publisher stack. Operator form of `mapT`.
public func <£^> <S, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stateful: Stateful<S, Publisher<A, F>>
) -> Stateful<S, Publisher<B, F>> {
    stateful.mapT(fn)
}

// (<&^>) :: Stateful<s, Publisher<a, f>> -> (a -> b) -> Stateful<s, Publisher<b, f>>

/// Functor map lifted through the transformer (container on the left) for the Stateful-over-Publisher stack. Operator form of `mapT`.
public func <&^> <S, A: Sendable, B: Sendable, F: Error>(
    _ stateful: Stateful<S, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Stateful<S, Publisher<B, F>> {
    stateful.mapT(fn)
}
