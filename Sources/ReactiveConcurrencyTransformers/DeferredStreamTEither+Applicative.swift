// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredStreamTEither: outer = DeferredStream, inner = Either
// Type: DeferredStream<Either<L, A>>

/// Applicative liftA2 for the DeferredStream-over-Either stack: runs both effects and combines their results with fn.
public func liftA2TDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<Either<L, A>>, DeferredStream<Either<L, B>>) -> DeferredStream<Either<L, C>> {
    { @Sendable sa, sb in
        liftA2DeferredStream { ea, eb -> Either<L, C> in
            switch (ea, eb) {
            case let (.right(a), .right(b)): .right(fn(a, b))
            case let (.left(l), _): .left(l)
            case let (_, .left(l)): .left(l)
            }
        }(sa, sb)
    }
}

/// Applicative apply for the DeferredStream-over-Either stack.
public func applyTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredStream<Either<L, @Sendable (A) -> B>>,
    _ values: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    liftA2DeferredStream { ef, ea -> Either<L, B> in
        switch (ef, ea) {
        case let (.right(f), .right(a)): .right(f(a))
        case let (.left(l), _): .left(l)
        case let (_, .left(l)): .left(l)
        }
    }(fns, values)
}
