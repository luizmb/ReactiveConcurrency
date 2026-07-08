// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: DeferredTask<[a]> -> (a -> DeferredTask<[b]>) -> DeferredTask<[b]>
public func >>- <A: Sendable, B: Sendable>(
    _ task: DeferredTask<[A]>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<[B]>
) -> DeferredTask<[B]> {
    flatMapTDeferredTaskArray(task, fn)
}

// (-<<) :: (a -> DeferredTask<[b]>) -> DeferredTask<[a]> -> DeferredTask<[b]>
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<[B]>,
    _ task: DeferredTask<[A]>
) -> DeferredTask<[B]> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask<[b]>) -> (b -> DeferredTask<[c]>) -> a -> DeferredTask<[c]>
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<[B]>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<[C]>
) -> @Sendable (A) -> DeferredTask<[C]> {
    kleisliTDeferredTaskArray(fn1, fn2)
}

// (<=<) :: (b -> DeferredTask<[c]>) -> (a -> DeferredTask<[b]>) -> a -> DeferredTask<[c]>
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ fn2: @escaping @Sendable (B) -> DeferredTask<[C]>,
    _ fn1: @escaping @Sendable (A) -> DeferredTask<[B]>
) -> @Sendable (A) -> DeferredTask<[C]> {
    fn1 >=> fn2
}
