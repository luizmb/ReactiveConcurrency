// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTResult: Alternative
// Type: DeferredTask<Result<A, E>>

// altT :: DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>>
// Lawful, left-biased Alternative: runs `lhs`; if it is `.success` that value wins and `rhs` is
// never run. Only when `lhs` is `.failure` is `rhs` run (its result — success or the "last"
// failure — is returned). Sequential and deterministic, so it is the instance `<|>` maps to.
// For the concurrent "first success, cancel the loser" behaviour use `raceDeferredTaskResult`.
public func altDeferredTaskResult<A: Sendable, E: Error & Sendable>(
    _ lhs: DeferredTask<Result<A, E>>,
    _ rhs: @autoclosure () -> DeferredTask<Result<A, E>>
) -> DeferredTask<Result<A, E>> {
    let captured = rhs()
    return DeferredTask {
        switch await lhs.run() {
        case let .success(value): .success(value)
        case .failure: await captured.run()
        }
    }
}

// raceDeferredTaskResult :: DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>> -> DeferredTask<Result<A,E>>
// Runs both tasks concurrently; returns the first `.success` and cancels the loser. If both fail,
// returns the last failure. NOT a lawful Alternative — the winner is scheduling-dependent, so it
// is exposed under `race`, not `<|>`.
public func raceDeferredTaskResult<A: Sendable, E: Error & Sendable>(
    _ lhs: DeferredTask<Result<A, E>>,
    _ rhs: @autoclosure () -> DeferredTask<Result<A, E>>
) -> DeferredTask<Result<A, E>> {
    let captured = rhs()
    return DeferredTask {
        await withTaskGroup(of: Result<A, E>.self) { group in
            group.addTask { await lhs.run() }
            group.addTask { await captured.run() }
            if let first = await group.next() {
                if case .success = first { group.cancelAll(); return first }
                if let second = await group.next() { return second }
                return first
            }
            // Unreachable: the group always has 2 tasks.
            return await lhs.run()
        }
    }
}
