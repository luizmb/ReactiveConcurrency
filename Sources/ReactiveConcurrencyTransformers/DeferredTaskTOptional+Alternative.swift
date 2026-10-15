// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredTaskTOptional: Alternative
// Type: DeferredTask<A?>

// altT :: DeferredTask<A?> -> DeferredTask<A?> -> DeferredTask<A?>
// Lawful, left-biased Alternative: runs `lhs`; if it is non-nil that value wins and `rhs` is
// never run. Only when `lhs` is nil is `rhs` run. Sequential and deterministic, so left identity
// and referential transparency hold — this is the instance `<|>` maps to.
// For the concurrent "first non-nil, cancel the loser" behaviour use `raceDeferredTaskOptional`.
/// Left-biased Alternative for the DeferredTask-over-Optional stack — lhs wins if non-nil, else rhs (sequential, lawful; the `<|>` instance).
func altDeferredTaskOptional<A: Sendable>(
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
/// Concurrent race for the DeferredTask-over-Optional stack — first non-nil wins, loser cancelled (scheduling-dependent, NOT a lawful Alternative).
func raceDeferredTaskOptional<A: Sendable>(
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

// MARK: - Stack surface

public extension DeferredTaskTOptional {
    /// Left-biased choice (Alternative `<|>`): runs `self`; if it is non-nil that value wins and
    /// `other` never runs, otherwise `other` runs. Sequential and lawful.
    /// - Parameter other: The fallback, built only when needed.
    /// - Returns: The first non-nil result, else `other`'s.
    func alt(_ other: @autoclosure () -> DeferredTaskTOptional<A>) -> DeferredTaskTOptional<A> {
        DeferredTaskTOptional(altDeferredTaskOptional(rawValue, other().rawValue))
    }

    /// Runs `self` and `other` concurrently: the first non-nil result wins and the loser is
    /// cancelled. Scheduling-dependent, so NOT a lawful Alternative (not `<|>`).
    /// - Parameter other: The competitor.
    /// - Returns: The first non-nil result, else the last one to finish.
    func race(_ other: @autoclosure () -> DeferredTaskTOptional<A>) -> DeferredTaskTOptional<A> {
        DeferredTaskTOptional(raceDeferredTaskOptional(rawValue, other().rawValue))
    }
}
