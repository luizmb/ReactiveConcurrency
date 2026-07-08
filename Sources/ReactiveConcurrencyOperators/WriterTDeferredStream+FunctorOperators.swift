// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>>
public func <£^> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    stream.mapT(fn)
}

// (<&^>) :: DeferredStream<Writer<w, a>> -> (a -> b) -> DeferredStream<Writer<w, b>>
public func <&^> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Writer<W, A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<Writer<W, B>> {
    stream.mapT(fn)
}
