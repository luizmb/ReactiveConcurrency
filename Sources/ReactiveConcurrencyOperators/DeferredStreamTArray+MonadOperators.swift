// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredStream<[a]> -> (a -> DeferredStream<[b]>) -> DeferredStream<[b]>

/// Monadic bind — sequences a dependent effect (container on the left) for the DeferredStream-over-Array stack. Operator form of `flatMap`.
public func >>- <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<[A]>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<[B]>
) -> DeferredStream<[B]> {
    flatMapTDeferredStreamArray(stream, fn)
}

// (-<<) :: (a -> DeferredStream<[b]>) -> DeferredStream<[a]> -> DeferredStream<[b]>

/// Monadic bind — sequences a dependent effect (function on the left) for the DeferredStream-over-Array stack. Operator form of `flatMap`.
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<[B]>,
    _ stream: DeferredStream<[A]>
) -> DeferredStream<[B]> {
    stream >>- fn
}

// (>=>) :: (a -> DeferredStream<[b]>) -> (b -> DeferredStream<[c]>) -> a -> DeferredStream<[c]>

/// Left-to-right Kleisli composition of two effectful functions for the DeferredStream-over-Array stack.
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<[B]>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<[C]>
) -> @Sendable (A) -> DeferredStream<[C]> {
    kleisliTDeferredStreamArray(fn1, fn2)
}

// (<=<) :: (b -> DeferredStream<[c]>) -> (a -> DeferredStream<[b]>) -> a -> DeferredStream<[c]>

/// Reverse Kleisli composition — `g <=< f == f >=> g` for the DeferredStream-over-Array stack.
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredStream<[C]>,
    _ fn1: @escaping @Sendable (A) -> DeferredStream<[B]>
) -> @Sendable (A) -> DeferredStream<[C]> {
    fn1 >=> fn2
}
