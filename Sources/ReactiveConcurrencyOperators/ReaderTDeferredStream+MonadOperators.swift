// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Reader<env, DeferredStream<a>> -> (a -> Reader<env, DeferredStream<b>>) -> Reader<env, DeferredStream<b>>

/// Monadic bind — sequences a dependent effect (container on the left) for the Reader-over-DeferredStream stack. Operator form of `flatMap`.
public func >>- <Env: Sendable, A: Sendable, B: Sendable>(
    _ reader: Reader<Env, DeferredStream<A>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>
) -> Reader<Env, DeferredStream<B>> {
    reader.flatMapT(fn)
}

// (-<<) :: (a -> Reader<env, DeferredStream<b>>) -> Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>>

/// Monadic bind — sequences a dependent effect (function on the left) for the Reader-over-DeferredStream stack. Operator form of `flatMap`.
public func -<< <Env: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>,
    _ reader: Reader<Env, DeferredStream<A>>
) -> Reader<Env, DeferredStream<B>> {
    reader.flatMapT(fn)
}

// (>=>) :: (a -> Reader<env, DeferredStream<b>>) -> (b -> Reader<env, DeferredStream<c>>) -> a -> Reader<env, DeferredStream<c>>

/// Left-to-right Kleisli composition of two effectful functions for the Reader-over-DeferredStream stack.
public func >=> <Env: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, DeferredStream<C>>
) -> @Sendable (A) -> Reader<Env, DeferredStream<C>> {
    kleisliTReaderDeferredStream(fn1, fn2)
}

// (<=<) :: (b -> Reader<env, DeferredStream<c>>) -> (a -> Reader<env, DeferredStream<b>>) -> a -> Reader<env, DeferredStream<c>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the Reader-over-DeferredStream stack.
public func <=< <Env: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> Reader<Env, DeferredStream<C>>,
    _ fn1: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>
) -> @Sendable (A) -> Reader<Env, DeferredStream<C>> {
    fn1 >=> fn2
}
