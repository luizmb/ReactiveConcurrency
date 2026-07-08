// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredStream: outer = Reader, inner = DeferredStream
// Type: Reader<Env, DeferredStream<A>>

public extension Reader {
    /// Functor map over the Reader-over-DeferredStream stack: transforms the innermost value, leaving the Reader and DeferredStream layers intact.
    func mapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> Reader<Environment, DeferredStream<B>>
    where Output == DeferredStream<A> {
        mapReader { stream in stream.map(fn) }
    }

    /// Functor map (point-free) for the Reader-over-DeferredStream stack: transforms the innermost value, leaving the Reader and DeferredStream
    /// layers intact.
    static func fmapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> @Sendable (Reader<Environment, DeferredStream<A>>) -> Reader<Environment, DeferredStream<B>>
    where Output == DeferredStream<A> {
        { $0.mapT(fn) }
    }
}
