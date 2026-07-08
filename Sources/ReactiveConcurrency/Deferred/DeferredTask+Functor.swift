// SPDX-License-Identifier: Apache-2.0

public extension DeferredTask {
    /// Transforms the eventual success value with `fn`, deferring the work until ``run()``.
    func map<B: Sendable>(_ fn: @escaping @Sendable (Success) -> B) -> DeferredTask<B> {
        DeferredTask<B> { await fn(self.run()) }
    }

    /// The curried, free-function form of ``map(_:)`` for point-free composition.
    static func fmap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> B
    ) -> @Sendable (DeferredTask<Success>) -> DeferredTask<B> {
        { @Sendable task in task.map(fn) }
    }

    /// Discards the success value, replacing it with `value` once the task runs.
    func replace<B: Sendable>(_ value: B) -> DeferredTask<B> {
        map { _ in value }
    }
}
