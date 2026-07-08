// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>>

/// Functor map lifted through the transformer (function on the left) for the Reader-over-DeferredStream stack. Operator form of `mapT`.
public func <£^> <Env, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ reader: Reader<Env, DeferredStream<A>>
) -> Reader<Env, DeferredStream<B>> {
    reader.mapT(fn)
}

// (<&^>) :: Reader<env, DeferredStream<a>> -> (a -> b) -> Reader<env, DeferredStream<b>>

/// Functor map lifted through the transformer (container on the left) for the Reader-over-DeferredStream stack. Operator form of `mapT`.
public func <&^> <Env, A: Sendable, B: Sendable>(
    _ reader: Reader<Env, DeferredStream<A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Reader<Env, DeferredStream<B>> {
    reader.mapT(fn)
}
