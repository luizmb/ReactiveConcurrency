// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredTaskTEither: outer = DeferredTask, inner = Either
// Type: DeferredTask<Either<L, A>>

/// Functor map over the DeferredTask-over-Either stack: transforms the innermost value, leaving the DeferredTask and Either layers intact.
public func mapTDeferredTaskEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Either<L, A>>
) -> DeferredTask<Either<L, B>> {
    task.map { either in either.mapRight(fn) }
}

/// Functor map (point-free) for the DeferredTask-over-Either stack: transforms the innermost value, leaving the DeferredTask and Either layers
/// intact.
public func fmapTDeferredTaskEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredTask<Either<L, A>>) -> DeferredTask<Either<L, B>> {
    { @Sendable task in mapTDeferredTaskEither(fn, task) }
}
