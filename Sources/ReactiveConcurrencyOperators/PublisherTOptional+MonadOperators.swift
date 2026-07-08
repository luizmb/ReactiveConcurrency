// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<a?, f> -> (a -> Publisher<b?, f>) -> Publisher<b?, f>

/// Monadic bind — sequences a dependent effect (container on the left) for the Publisher-over-Optional stack. Operator form of `flatMap`.
public func >>- <A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<A?, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<B?, F>
) -> Publisher<B?, F> {
    flatMapTPublisherOptional(publisher, fn)
}

// (-<<) :: (a -> Publisher<b?, f>) -> Publisher<a?, f> -> Publisher<b?, f>

/// Monadic bind — sequences a dependent effect (function on the left) for the Publisher-over-Optional stack. Operator form of `flatMap`.
public func -<< <A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<B?, F>,
    _ publisher: Publisher<A?, F>
) -> Publisher<B?, F> {
    publisher >>- fn
}

// (>=>) :: (a -> Publisher<b?, f>) -> (b -> Publisher<c?, f>) -> a -> Publisher<c?, f>

/// Left-to-right Kleisli composition of two effectful functions for the Publisher-over-Optional stack.
public func >=> <A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<B?, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<C?, F>
) -> @Sendable (A) -> Publisher<C?, F> {
    kleisliTPublisherOptional(fn1, fn2)
}

// (<=<) :: (b -> Publisher<c?, f>) -> (a -> Publisher<b?, f>) -> a -> Publisher<c?, f>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the Publisher-over-Optional stack.
public func <=< <A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn2: @escaping @Sendable (B) -> Publisher<C?, F>,
    _ fn1: @escaping @Sendable (A) -> Publisher<B?, F>
) -> @Sendable (A) -> Publisher<C?, F> {
    fn1 >=> fn2
}
