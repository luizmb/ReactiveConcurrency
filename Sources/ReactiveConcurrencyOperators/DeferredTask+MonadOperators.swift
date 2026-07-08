// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (>>-) :: DeferredTask a -> (a -> DeferredTask b) -> DeferredTask b

/// Monadic bind — sequences a dependent effect (container on the left). Operator form of `flatMap`.
public func >>- <A: Sendable, B: Sendable>(
    _ task: DeferredTask<A>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<B>
) -> DeferredTask<B> {
    task.flatMap(fn)
}

// (-<<) :: (a -> DeferredTask b) -> DeferredTask a -> DeferredTask b

/// Monadic bind — sequences a dependent effect (function on the left). Operator form of `flatMap`.
public func -<< <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<B>,
    _ task: DeferredTask<A>
) -> DeferredTask<B> {
    task >>- fn
}

// (>=>) :: (a -> DeferredTask b) -> (b -> DeferredTask c) -> (a -> DeferredTask c)

/// Left-to-right Kleisli composition of two effectful functions.
public func >=> <A: Sendable, B: Sendable, C: Sendable>(
    _ f: @escaping @Sendable (A) -> DeferredTask<B>,
    _ g: @escaping @Sendable (B) -> DeferredTask<C>
) -> @Sendable (A) -> DeferredTask<C> {
    DeferredTask<A>.kleisli(f, g)
}

// (<=<) :: (b -> DeferredTask c) -> (a -> DeferredTask b) -> (a -> DeferredTask c)

/// Reverse Kleisli composition — `g <=< f == f >=> g`.
public func <=< <A: Sendable, B: Sendable, C: Sendable>(
    _ g: @escaping @Sendable (B) -> DeferredTask<C>,
    _ f: @escaping @Sendable (A) -> DeferredTask<B>
) -> @Sendable (A) -> DeferredTask<C> {
    DeferredTask<B>.kleisliBack(g, f)
}
