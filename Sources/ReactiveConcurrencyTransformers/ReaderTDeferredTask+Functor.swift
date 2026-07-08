// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredTask: outer = Reader, inner = DeferredTask
// Type: Reader<Env, DeferredTask<A>>

public extension Reader {
    /// Functor map over the Reader-over-DeferredTask stack: transforms the innermost value, leaving the Reader and DeferredTask layers intact.
    func mapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> Reader<Environment, DeferredTask<B>>
    where Output == DeferredTask<A> {
        mapReader { task in task.map(fn) }
    }

    /// Functor map (point-free) for the Reader-over-DeferredTask stack: transforms the innermost value, leaving the Reader and DeferredTask layers
    /// intact.
    static func fmapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> @Sendable (Reader<Environment, DeferredTask<A>>) -> Reader<Environment, DeferredTask<B>>
    where Output == DeferredTask<A> {
        { $0.mapT(fn) }
    }
}
