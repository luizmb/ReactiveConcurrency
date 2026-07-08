// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (>>-) :: Publisher a e -> (a -> Publisher b e) -> Publisher b e
public func >>- <A: Sendable, B: Sendable, E: Error>(
    _ publisher: Publisher<A, E>,
    _ fn: @escaping @Sendable (A) -> Publisher<B, E>
) -> Publisher<B, E> {
    publisher.flatMap(fn)
}

// (-<<) :: (a -> Publisher b e) -> Publisher a e -> Publisher b e
public func -<< <A: Sendable, B: Sendable, E: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<B, E>,
    _ publisher: Publisher<A, E>
) -> Publisher<B, E> {
    publisher >>- fn
}

// (>=>) :: (a -> Publisher b e) -> (b -> Publisher c e) -> (a -> Publisher c e)
public func >=> <A: Sendable, B: Sendable, C: Sendable, E: Error>(
    _ f: @escaping @Sendable (A) -> Publisher<B, E>,
    _ g: @escaping @Sendable (B) -> Publisher<C, E>
) -> @Sendable (A) -> Publisher<C, E> {
    Publisher<A, E>.kleisli(f, g)
}

// (<=<) :: (b -> Publisher c e) -> (a -> Publisher b e) -> (a -> Publisher c e)
// Reverse Kleisli — restores the symmetry base DeferredTask/DeferredStream already had.
public func <=< <A: Sendable, B: Sendable, C: Sendable, E: Error>(
    _ g: @escaping @Sendable (B) -> Publisher<C, E>,
    _ f: @escaping @Sendable (A) -> Publisher<B, E>
) -> @Sendable (A) -> Publisher<C, E> {
    Publisher<B, E>.kleisliBack(g, f)
}
