// SPDX-License-Identifier: Apache-2.0

public extension DeferredTask {
    /// Lifts a plain value into an already-resolved task (the applicative `pure`).
    static func pure(_ value: Success) -> DeferredTask<Success> {
        DeferredTask { value }
    }

    /// Runs `self` then `rhs` sequentially, keeping only `rhs`'s result.
    func seqRight<B: Sendable>(_ rhs: DeferredTask<B>) -> DeferredTask<B> {
        liftA2DeferredTask { _, b in b }(self, rhs)
    }

    /// Runs `self` then `rhs` sequentially, keeping only `self`'s result.
    func seqLeft<B: Sendable>(_ rhs: DeferredTask<B>) -> DeferredTask<Success> {
        liftA2DeferredTask { a, _ in a }(self, rhs)
    }

    /// Runs the given tasks sequentially left-to-right, collecting their results into a tuple.
    static func zip<B: Sendable, each C: Sendable>(
        _ first: DeferredTask<Success>,
        _ second: DeferredTask<B>,
        _ additional: repeat DeferredTask<each C>
    ) -> DeferredTask<(Success, B, repeat each C)> {
        DeferredTask<(Success, B, repeat each C)> {
            (await first.run(), await second.run(), repeat await (each additional).run())
        }
    }
}

/// Applies a deferred function to a deferred value (applicative `<*>`).
///
/// Sequential and lawful: runs `fns` then `values`, equivalent to
/// `fns >>= { f in values >>= { a in pure(f(a)) } }`.
public func applyDeferredTask<A: Sendable, B: Sendable>(
    _ fns: DeferredTask<@Sendable (A) -> B>,
    _ values: DeferredTask<A>
) -> DeferredTask<B> {
    DeferredTask<B> {
        let f = await fns.run()
        let a = await values.run()
        return f(a)
    }
}

/// Combines two deferred tasks with a binary function, running them sequentially left-to-right.
public func liftA2DeferredTask<A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredTask<A>, DeferredTask<B>) -> DeferredTask<C> {
    { @Sendable ta, tb in
        DeferredTask<C> {
            let a = await ta.run()
            let b = await tb.run()
            return fn(a, b)
        }
    }
}
