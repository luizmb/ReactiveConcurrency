// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredStream: outer = Reader, inner = DeferredStream
// Type: Reader<Env, DeferredStream<A>>

extension Reader {
    /// Monadic bind for the Reader-over-DeferredStream stack: threads the inner value through fn, re-reading the shared environment.
    func flatMapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, DeferredStream<B>>
    ) -> Reader<Environment, DeferredStream<B>>
    where Output == DeferredStream<A>, Environment: Sendable {
        Reader<Environment, DeferredStream<B>> { env in
            self.runReader(env).flatMap { a in fn(a).runReader(env) }
        }
    }
}
