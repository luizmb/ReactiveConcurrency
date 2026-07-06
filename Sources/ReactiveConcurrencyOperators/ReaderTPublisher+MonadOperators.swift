// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Reader<env, Publisher<a, f>> -> (a -> Reader<env, Publisher<b, f>>) -> Reader<env, Publisher<b, f>>
public func >>- <Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ reader: Reader<Env, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}

// (-<<) :: (a -> Reader<env, Publisher<b, f>>) -> Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>>
public func -<< <Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>,
    _ reader: Reader<Env, Publisher<A, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}

// (>=>) :: (a -> Reader<env, Publisher<b, f>>) -> (b -> Reader<env, Publisher<c, f>>) -> a -> Reader<env, Publisher<c, f>>
public func >=> <Env: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, Publisher<C, F>>
) -> @Sendable (A) -> Reader<Env, Publisher<C, F>> {
    { a in fn1(a).flatMapT(fn2) }
}
