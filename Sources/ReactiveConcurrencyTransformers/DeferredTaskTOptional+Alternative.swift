// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: Alternative
// Type: DeferredTask<A?>

// altT :: DeferredTask<A?> -> DeferredTask<A?> -> DeferredTask<A?>
// Lawful, left-biased Alternative: runs `lhs`; if it is non-nil that value wins and `rhs` is
// never run. Only when `lhs` is nil is `rhs` run. Sequential and deterministic, so left identity
// and referential transparency hold — this is the instance `<|>` maps to.
// For the concurrent "first non-nil, cancel the loser" behaviour use `raceDeferredTaskOptional`.
public func altDeferredTaskOptional<A: Sendable>(
    _ lhs: DeferredTask<A?>,
    _ rhs: @autoclosure () -> DeferredTask<A?>
) -> DeferredTask<A?> {
    let captured = rhs()
    return DeferredTask {
        if let value = await lhs.run() { value } else { await captured.run() }
    }
}

// raceDeferredTaskOptional :: DeferredTask<A?> -> DeferredTask<A?> -> DeferredTask<A?>
// Runs both tasks concurrently; returns the first non-nil result and cancels the loser.
// If both return nil, returns nil. NOT a lawful Alternative — the winner is scheduling-dependent,
// so it is exposed under `race`, not `<|>`.
public func raceDeferredTaskOptional<A: Sendable>(
    _ lhs: DeferredTask<A?>,
    _ rhs: @autoclosure () -> DeferredTask<A?>
) -> DeferredTask<A?> {
    let captured = rhs()
    return DeferredTask {
        await withTaskGroup(of: A?.self) { group in
            group.addTask { await lhs.run() }
            group.addTask { await captured.run() }
            for await value in group {
                if let v = value { group.cancelAll(); return v }
            }
            return nil
        }
    }
}
