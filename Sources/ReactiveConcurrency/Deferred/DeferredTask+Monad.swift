// SPDX-License-Identifier: Apache-2.0

public extension DeferredTask {
    /// Chains a dependent task, feeding this task's result into `fn` (monadic `bind`).
    func flatMap<B: Sendable>(_ fn: @escaping @Sendable (Success) -> DeferredTask<B>) -> DeferredTask<B> {
        DeferredTask<B> { await fn(await self.run()).run() }
    }

    /// The curried, free-function form of `flatMap` for point-free composition.
    static func flatMap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> DeferredTask<B>
    ) -> @Sendable (DeferredTask<Success>) -> DeferredTask<B> {
        { @Sendable task in task.flatMap(fn) }
    }

    /// Flattens a task of a task into a single task (monadic `join`).
    static func join<A: Sendable>(_ nested: DeferredTask<DeferredTask<A>>) -> DeferredTask<A>
    where Success == DeferredTask<A> {
        nested.flatMap { $0 }
    }

    /// Discards the success value, yielding a `DeferredTask<Void>`.
    func void() -> DeferredTask<Void> {
        map { _ in }
    }

    /// Left-to-right Kleisli composition: `f` then `g`, threading the effect through both.
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Success) -> DeferredTask<B>,
        _ g: @escaping @Sendable (B) -> DeferredTask<C>
    ) -> @Sendable (Success) -> DeferredTask<C> {
        { @Sendable a in f(a).flatMap(g) }
    }

    /// Right-to-left Kleisli composition: applies `f` first, then `g`.
    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Success) -> DeferredTask<B>,
        _ f: @escaping @Sendable (X) -> DeferredTask<Success>
    ) -> @Sendable (X) -> DeferredTask<B> {
        { @Sendable x in f(x).flatMap(g) }
    }
}
