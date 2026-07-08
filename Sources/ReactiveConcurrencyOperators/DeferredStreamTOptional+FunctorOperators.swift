// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> DeferredStream<a?> -> DeferredStream<b?>

/// Functor map lifted through the transformer (function on the left) for the DeferredStream-over-Optional stack. Operator form of `mapT`.
public func <£^> <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<A?>
) -> DeferredStream<B?> {
    mapTDeferredStreamOptional(fn, stream)
}

// (<&^>) :: DeferredStream<a?> -> (a -> b) -> DeferredStream<b?>

/// Functor map lifted through the transformer (container on the left) for the DeferredStream-over-Optional stack. Operator form of `mapT`.
public func <&^> <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A?>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<B?> {
    mapTDeferredStreamOptional(fn, stream)
}
