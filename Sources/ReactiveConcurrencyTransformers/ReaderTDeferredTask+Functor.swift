// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredTask: outer = Reader, inner = DeferredTask
// Type: Reader<Env, DeferredTask<A>>

extension Reader {
    /// Functor map over the Reader-over-DeferredTask stack: transforms the innermost value, leaving the Reader and DeferredTask layers intact.
    func mapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> Reader<Environment, DeferredTask<B>>
    where Output == DeferredTask<A> {
        mapReader { task in task.map(fn) }
    }
}
