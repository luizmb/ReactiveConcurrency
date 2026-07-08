// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredTaskTEither: outer = DeferredTask, inner = Either
// Type: DeferredTask<Either<L, A>>

// flatMapT: .left short-circuits; .right(a) proceeds through fn
public func flatMapTDeferredTaskEither<L: Sendable, A: Sendable, B: Sendable>(
    _ task: DeferredTask<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> DeferredTask<Either<L, B>> {
    task.flatMap { either in
        switch either {
        case let .right(a): fn(a)
        case let .left(l): .pure(.left(l))
        }
    }
}

public func bindTDeferredTaskEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>
) -> @Sendable (DeferredTask<Either<L, A>>) -> DeferredTask<Either<L, B>> {
    { @Sendable task in flatMapTDeferredTaskEither(task, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
public func kleisliTDeferredTaskEither<L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Either<L, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Either<L, C>>
) -> @Sendable (A) -> DeferredTask<Either<L, C>> {
    { @Sendable a in flatMapTDeferredTaskEither(fn1(a), fn2) }
}
