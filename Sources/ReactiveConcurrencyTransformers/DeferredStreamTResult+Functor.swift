// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTResult: outer = DeferredStream, inner = Result
// Type: DeferredStream<Result<A, E>>  — Haskell: ExceptT e DeferredStream

/// Functor map over the DeferredStream-over-Result stack: transforms the innermost value, leaving the DeferredStream and Result layers intact.
func mapTDeferredStreamResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Result<A, E>>
) -> DeferredStream<Result<B, E>> {
    stream.map { result in result.map(fn) }
}
