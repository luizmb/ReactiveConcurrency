// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredTask: outer = Reader, inner = DeferredTask
// Type: Reader<Env, DeferredTask<A>>

extension Reader {
    /// Monadic bind for the Reader-over-DeferredTask stack: threads the inner value through fn, re-reading the shared environment.
    func flatMapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, DeferredTask<B>>
    ) -> Reader<Environment, DeferredTask<B>>
    where Output == DeferredTask<A>, Environment: Sendable {
        Reader<Environment, DeferredTask<B>> { env in
            self.runReader(env).flatMap { a in fn(a).runReader(env) }
        }
    }
}
