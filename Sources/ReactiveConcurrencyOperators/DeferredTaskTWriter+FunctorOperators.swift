// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>>

/// Functor map lifted through the transformer (function on the left) for the DeferredTask-over-Writer stack. Operator form of `mapT`.
public func <£^> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Writer<W, A>>
) -> DeferredTask<Writer<W, B>> {
    task.mapT(fn)
}

// (<&^>) :: DeferredTask<Writer<w, a>> -> (a -> b) -> DeferredTask<Writer<w, b>>

/// Functor map lifted through the transformer (container on the left) for the DeferredTask-over-Writer stack. Operator form of `mapT`.
public func <&^> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Writer<W, A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredTask<Writer<W, B>> {
    task.mapT(fn)
}
