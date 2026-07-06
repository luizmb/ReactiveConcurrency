// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Reader<env, Publisher<a, f>> -> Reader<env, Publisher<b, f>>
public func <£^> <Env, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ reader: Reader<Env, Publisher<A, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.mapT(fn)
}

// (<&^>) :: Reader<env, Publisher<a, f>> -> (a -> b) -> Reader<env, Publisher<b, f>>
public func <&^> <Env, A: Sendable, B: Sendable, F: Error>(
    _ reader: Reader<Env, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Reader<Env, Publisher<B, F>> {
    reader.mapT(fn)
}
