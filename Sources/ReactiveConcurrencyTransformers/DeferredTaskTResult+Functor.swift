// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTResult: outer = DeferredTask, inner = Result
// Type: DeferredTask<Result<A, E>>  — Haskell: ExceptT e DeferredTask

/// Functor map over the DeferredTask-over-Result stack: transforms the innermost value, leaving the DeferredTask and Result layers intact.
public func mapTDeferredTaskResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ task: DeferredTask<Result<A, E>>
) -> DeferredTask<Result<B, E>> {
    task.map { result in result.map(fn) }
}

/// Functor map (point-free) for the DeferredTask-over-Result stack: transforms the innermost value, leaving the DeferredTask and Result layers
/// intact.
public func fmapTDeferredTaskResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredTask<Result<A, E>>) -> DeferredTask<Result<B, E>> {
    { @Sendable task in mapTDeferredTaskResult(fn, task) }
}
