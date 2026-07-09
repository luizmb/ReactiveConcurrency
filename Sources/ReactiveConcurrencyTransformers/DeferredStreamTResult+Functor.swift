// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTResult: outer = DeferredStream, inner = Result
// Type: DeferredStream<Result<A, E>>  — Haskell: ExceptT e DeferredStream

/// Functor map over the DeferredStream-over-Result stack: transforms the innermost value, leaving the DeferredStream and Result layers intact.
public func mapTDeferredStreamResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Result<A, E>>
) -> DeferredStream<Result<B, E>> {
    stream.map { result in result.map(fn) }
}

/// Functor map (point-free) for the DeferredStream-over-Result stack: transforms the innermost value, leaving the DeferredStream and Result layers
/// intact.
public func fmapTDeferredStreamResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<Result<A, E>>) -> DeferredStream<Result<B, E>> {
    { @Sendable stream in mapTDeferredStreamResult(fn, stream) }
}
