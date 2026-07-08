// SPDX-License-Identifier: Apache-2.0

// Traversals for DeferredTask: turn a container of tasks into a task of a container, or map each
// element to a task and collect. DeferredTask is a sequential applicative, so these run in order.

// sequence :: [DeferredTask a] -> DeferredTask [a]
// Runs the tasks left-to-right and collects their results (order preserved).
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

// traverse :: [a] -> (a -> DeferredTask b) -> DeferredTask [b]
public func traverseDeferredTask<A: Sendable, B: Sendable>(
    _ xs: [A],
    _ transform: @escaping @Sendable (A) -> DeferredTask<B>
) -> DeferredTask<[B]> {
    sequenceDeferredTask(xs.map(transform))
}

// sequence :: DeferredTask a? -> DeferredTask a?   (nil short-circuits to a pure nil task)
public func sequenceDeferredTask<A: Sendable>(_ task: DeferredTask<A>?) -> DeferredTask<A?> {
    guard let task else { return DeferredTask { nil } }
    return task.map { Optional($0) }
}

// traverse :: a? -> (a -> DeferredTask b) -> DeferredTask b?
public func traverseDeferredTask<A: Sendable, B: Sendable>(
    _ x: A?,
    _ transform: @escaping @Sendable (A) -> DeferredTask<B>
) -> DeferredTask<B?> {
    guard let x else { return DeferredTask { nil } }
    return transform(x).map { Optional($0) }
}
