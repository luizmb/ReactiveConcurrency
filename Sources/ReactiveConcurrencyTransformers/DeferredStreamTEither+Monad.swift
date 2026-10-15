// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredStreamTEither: outer = DeferredStream, inner = Either
// Type: DeferredStream<Either<L, A>>

// flatMapT: .left propagates; .right(a) proceeds through fn
/// Monadic bind for the DeferredStream-over-Either stack: .left short-circuits; .right threads through fn.
func flatMapTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable>(
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
