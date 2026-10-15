// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredStream: outer = Reader, inner = DeferredStream
// Type: Reader<Env, DeferredStream<A>>

extension Reader {
    /// Functor map over the Reader-over-DeferredStream stack: transforms the innermost value, leaving the Reader and DeferredStream layers intact.
    func mapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> Reader<Environment, DeferredStream<B>>
    where Output == DeferredStream<A> {
        mapReader { stream in stream.map(fn) }
    }
}
