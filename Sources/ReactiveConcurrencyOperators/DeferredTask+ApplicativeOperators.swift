// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<*>) :: DeferredTask (a -> b) -> DeferredTask a -> DeferredTask b

/// Applicative apply — applies a wrapped function to a wrapped value.
public func <*> <A: Sendable, B: Sendable>(
    _ fns: DeferredTask<@Sendable (A) -> B>,
    _ values: DeferredTask<A>
) -> DeferredTask<B> {
    applyDeferredTask(fns, values)
}

// (*>) :: DeferredTask a -> DeferredTask b -> DeferredTask b

/// Sequences two effects, keeping the right result. Operator form of `seqRight`.
public func *> <A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<A>,
    _ rhs: DeferredTask<B>
) -> DeferredTask<B> {
    lhs.seqRight(rhs)
}

// (<*) :: DeferredTask a -> DeferredTask b -> DeferredTask a

/// Sequences two effects, keeping the left result. Operator form of `seqLeft`.
public func <* <A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<A>,
    _ rhs: DeferredTask<B>
) -> DeferredTask<A> {
    lhs.seqLeft(rhs)
}
