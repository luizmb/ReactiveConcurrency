// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredTask<a?> -> DeferredTask<b?>

/// Functor map lifted through the transformer (function on the left) for the DeferredTask-over-Optional stack. Operator form of `mapT`.
public func <£^> <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<A?>
) -> DeferredTask<B?> {
    mapTDeferredTaskOptional(fn, task)
}

// (<&^>) :: DeferredTask<a?> -> (a -> b) -> DeferredTask<b?>

/// Functor map lifted through the transformer (container on the left) for the DeferredTask-over-Optional stack. Operator form of `mapT`.
public func <&^> <A: Sendable, B: Sendable>(
    _ task: DeferredTask<A?>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredTask<B?> {
    mapTDeferredTaskOptional(fn, task)
}
