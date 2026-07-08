// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredStreamTEither: outer = DeferredStream, inner = Either
// Type: DeferredStream<Either<L, A>>

// flatMapT: .left propagates; .right(a) proceeds through fn
public func flatMapTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable>(
    _ stream: DeferredStream<Either<L, A>>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>
) -> DeferredStream<Either<L, B>> {
    let s = stream
    return DeferredStream<Either<L, B>> {
        AsyncStream<Either<L, B>> { continuation in
            let task = Task { @Sendable in
                for await either in s {
                    switch either {
                    case let .right(a):
                        for await b in fn(a) {
                            continuation.yield(b)
                        }
                    case let .left(l):
                        continuation.yield(.left(l))
                    }
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

public func bindTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>
) -> @Sendable (DeferredStream<Either<L, A>>) -> DeferredStream<Either<L, B>> {
    { @Sendable stream in flatMapTDeferredStreamEither(stream, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
public func kleisliTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Either<L, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Either<L, C>>
) -> @Sendable (A) -> DeferredStream<Either<L, C>> {
    { @Sendable a in flatMapTDeferredStreamEither(fn1(a), fn2) }
}
