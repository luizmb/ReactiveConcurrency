// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Result<a,e>, f> -> (a -> Publisher<Result<b,e>, f>) -> Publisher<Result<b,e>, f>
public func >>- <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ publisher: Publisher<Result<A, E>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> Publisher<Result<B, E>, F> {
    flatMapTPublisherResult(publisher, fn)
}

// (-<<) :: (a -> Publisher<Result<b,e>, f>) -> Publisher<Result<a,e>, f> -> Publisher<Result<b,e>, f>
public func -<< <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>,
    _ publisher: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    publisher >>- fn
}

// (>=>) :: (a -> Publisher<Result<b,e>, f>) -> (b -> Publisher<Result<c,e>, f>) -> a -> Publisher<Result<c,e>, f>
public func >=> <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<Result<C, E>, F>
) -> @Sendable (A) -> Publisher<Result<C, E>, F> {
    kleisliTPublisherResult(fn1, fn2)
}

// (<=<) :: (b -> Publisher<Result<c,e>, f>) -> (a -> Publisher<Result<b,e>, f>) -> a -> Publisher<Result<c,e>, f>
public func <=< <A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable, F: Error>(
    _ fn2: @escaping @Sendable (B) -> Publisher<Result<C, E>, F>,
    _ fn1: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> @Sendable (A) -> Publisher<Result<C, E>, F> {
    fn1 >=> fn2
}
