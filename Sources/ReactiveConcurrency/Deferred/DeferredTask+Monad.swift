// SPDX-License-Identifier: Apache-2.0

public extension DeferredTask {
    // bind / flatMap :: DeferredTask a -> (a -> DeferredTask b) -> DeferredTask b
    func flatMap<B: Sendable>(_ fn: @escaping @Sendable (Success) -> DeferredTask<B>) -> DeferredTask<B> {
        DeferredTask<B> { await fn(await self.run()).run() }
    }

    static func flatMap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> DeferredTask<B>
    ) -> @Sendable (DeferredTask<Success>) -> DeferredTask<B> {
        { @Sendable task in task.flatMap(fn) }
    }

    // join :: DeferredTask (DeferredTask a) -> DeferredTask a
    static func join<A: Sendable>(_ nested: DeferredTask<DeferredTask<A>>) -> DeferredTask<A>
    where Success == DeferredTask<A> {
        nested.flatMap { $0 }
    }

    // void :: DeferredTask a -> DeferredTask ()
    func void() -> DeferredTask<Void> {
        map { _ in }
    }

    // kleisli :: (a -> DeferredTask b) -> (b -> DeferredTask c) -> (a -> DeferredTask c)
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Success) -> DeferredTask<B>,
        _ g: @escaping @Sendable (B) -> DeferredTask<C>
    ) -> @Sendable (Success) -> DeferredTask<C> {
        { @Sendable a in f(a).flatMap(g) }
    }

    // kleisliBack :: (b -> DeferredTask c) -> (a -> DeferredTask b) -> (a -> DeferredTask c)
    // Reverse Kleisli composition: f first, then g.
    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Success) -> DeferredTask<B>,
        _ f: @escaping @Sendable (X) -> DeferredTask<Success>
    ) -> @Sendable (X) -> DeferredTask<B> {
        { @Sendable x in f(x).flatMap(g) }
    }
}
