// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Reader<env, Publisher<a, f>> -> (a -> Reader<env, Publisher<b, f>>) -> Reader<env, Publisher<b, f>>

/// Monadic bind — sequences a dependent effect (container on the left) for the Reader-over-Publisher stack. Operator form of `flatMap`.
public func >>- <Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ reader: Reader<Env, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}

// (-<<) :: (a -> Reader<env, Publisher<b, f>>) -> Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>>

/// Monadic bind — sequences a dependent effect (function on the left) for the Reader-over-Publisher stack. Operator form of `flatMap`.
public func -<< <Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>,
    _ reader: Reader<Env, Publisher<A, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}

// (>=>) :: (a -> Reader<env, Publisher<b, f>>) -> (b -> Reader<env, Publisher<c, f>>) -> a -> Reader<env, Publisher<c, f>>

/// Left-to-right Kleisli composition of two effectful functions for the Reader-over-Publisher stack.
public func >=> <Env: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, Publisher<C, F>>
) -> @Sendable (A) -> Reader<Env, Publisher<C, F>> {
    kleisliTReaderPublisher(fn1, fn2)
}

// (<=<) :: (b -> Reader<env, Publisher<c, f>>) -> (a -> Reader<env, Publisher<b, f>>) -> a -> Reader<env, Publisher<c, f>>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the Reader-over-Publisher stack.
public func <=< <Env: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn2: @escaping @Sendable (B) -> Reader<Env, Publisher<C, F>>,
    _ fn1: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>
) -> @Sendable (A) -> Reader<Env, Publisher<C, F>> {
    fn1 >=> fn2
}
