// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTResult: outer = DeferredTask, inner = Result
// Type: DeferredTask<Result<A, E>>

// flatMapT :: DeferredTask<Result<A,E>> -> (A -> DeferredTask<Result<B,E>>) -> DeferredTask<Result<B,E>>
// failure short-circuits; success proceeds through fn
/// Monadic bind for the DeferredTask-over-Result stack: .failure short-circuits; .success threads through fn.
public func flatMapTDeferredTaskResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ task: DeferredTask<Result<A, E>>,
    _ fn: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>
) -> DeferredTask<Result<B, E>> {
    task.flatMap { result in
        switch result {
        case let .success(a): fn(a)
        case let .failure(e): .pure(.failure(e))
        }
    }
}

/// Monadic bind (point-free) for the DeferredTask-over-Result stack: .failure short-circuits; .success threads through fn.
public func bindTDeferredTaskResult<A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>
) -> @Sendable (DeferredTask<Result<A, E>>) -> DeferredTask<Result<B, E>> {
    { @Sendable task in flatMapTDeferredTaskResult(task, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
/// Left-to-right Kleisli composition for the DeferredTask-over-Result stack.
public func kleisliTDeferredTaskResult<A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredTask<Result<B, E>>,
    _ fn2: @escaping @Sendable (B) -> DeferredTask<Result<C, E>>
) -> @Sendable (A) -> DeferredTask<Result<C, E>> {
    { @Sendable a in flatMapTDeferredTaskResult(fn1(a), fn2) }
}
