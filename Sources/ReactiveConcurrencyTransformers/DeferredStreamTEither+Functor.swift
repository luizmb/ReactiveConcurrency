// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// DeferredStreamTEither: outer = DeferredStream, inner = Either
// Type: DeferredStream<Either<L, A>>  — Haskell: ExceptT l DeferredStream

/// Functor map over the DeferredStream-over-Either stack: transforms the innermost value, leaving the DeferredStream and Either layers intact.
func mapTDeferredStreamEither<L: Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    stream.map { either in either.mapRight(fn) }
}
