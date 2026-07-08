// SPDX-License-Identifier: Apache-2.0

// Traversals for DeferredTask: turn a container of tasks into a task of a container, or map each
// element to a task and collect. DeferredTask is a sequential applicative, so these run in order.

/// Turns an array of tasks into one task of an array, running them left-to-right (order preserved).
public func sequenceDeferredTask<A: Sendable>(_ tasks: [DeferredTask<A>]) -> DeferredTask<[A]> {
    DeferredTask<[A]> {
        var out: [A] = []
        out.reserveCapacity(tasks.count)
        for task in tasks {
            out.append(await task.run())
        }
        return out
    }
}

/// Maps each element to a task and collects the results into one task, running them in order.
public func traverseDeferredTask<A: Sendable, B: Sendable>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> DeferredTask<B>
) -> DeferredTask<[B]> {
    sequenceDeferredTask(xs.map(transform))
}

/// Flips an optional task into a task of an optional; `nil` short-circuits to a pure `nil` task.
public func sequenceDeferredTask<A: Sendable>(_ task: DeferredTask<A>?) -> DeferredTask<A?> {
    guard let task else { return DeferredTask { nil } }
    return task.map { Optional($0) }
}

/// Maps an optional value through a task-producing function; `nil` yields a pure `nil` task.
public func traverseDeferredTask<A: Sendable, B: Sendable>(
    _ x: A?,
    _ transform: @escaping @Sendable (A) -> DeferredTask<B>
) -> DeferredTask<B?> {
    guard let x else { return DeferredTask { nil } }
    return transform(x).map { Optional($0) }
}
