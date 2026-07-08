// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Reader<env, DeferredTask<a>> -> (a -> Reader<env, DeferredTask<b>>) -> Reader<env, DeferredTask<b>>

/// Monadic bind — sequences a dependent effect (container on the left) for the Reader-over-DeferredTask stack. Operator form of `flatMap`.
public func >>- <Env: Sendable, A: Sendable, B: Sendable>(
    _ reader: Reader<Env, DeferredTask<A>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, DeferredTask<B>>
) -> Reader<Env, DeferredTask<B>> {
    reader.flatMapT(fn)
}

// (-<<) :: (a -> Reader<env, DeferredTask<b>>) -> Reader<env, DeferredTask<a>> -> Reader<env, DeferredTask<b>>

/// Monadic bind — sequences a dependent effect (function on the left) for the Reader-over-DeferredTask stack. Operator form of `flatMap`.
public func -<< <Env: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> Reader<Env, DeferredTask<B>>,
    _ reader: Reader<Env, DeferredTask<A>>
) -> Reader<Env, DeferredTask<B>> {
    reader.flatMapT(fn)
}

// (>=>) :: (a -> Reader<env, DeferredTask<b>>) -> (b -> Reader<env, DeferredTask<c>>) -> a -> Reader<env, DeferredTask<c>>

/// Left-to-right Kleisli composition of two effectful functions for the Reader-over-DeferredTask stack.
public func >=> <Env: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, DeferredTask<B>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, DeferredTask<C>>
) -> @Sendable (A) -> Reader<Env, DeferredTask<C>> {
    kleisliTReaderDeferredTask(fn1, fn2)
}

// (<=<) :: (b -> Reader<env, DeferredTask<c>>) -> (a -> Reader<env, DeferredTask<b>>) -> a -> Reader<env, DeferredTask<c>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the Reader-over-DeferredTask stack.
public func <=< <Env: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> Reader<Env, DeferredTask<C>>,
    _ fn1: @escaping @Sendable (A) -> Reader<Env, DeferredTask<B>>
) -> @Sendable (A) -> Reader<Env, DeferredTask<C>> {
    fn1 >=> fn2
}
