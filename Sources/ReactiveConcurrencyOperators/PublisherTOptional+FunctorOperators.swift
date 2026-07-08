// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Publisher<a?, f> -> Publisher<b?, f>

/// Functor map lifted through the transformer (function on the left) for the Publisher-over-Optional stack. Operator form of `mapT`.
public func <£^> <A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<A?, F>
) -> Publisher<B?, F> {
    mapTPublisherOptional(fn, publisher)
}

// (<&^>) :: Publisher<a?, f> -> (a -> b) -> Publisher<b?, f>

/// Functor map lifted through the transformer (container on the left) for the Publisher-over-Optional stack. Operator form of `mapT`.
public func <&^> <A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<A?, F>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<B?, F> {
    mapTPublisherOptional(fn, publisher)
}
