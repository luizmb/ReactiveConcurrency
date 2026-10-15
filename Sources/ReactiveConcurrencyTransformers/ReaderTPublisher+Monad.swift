// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTPublisher: outer = Reader, inner = Publisher
// Type: Reader<Env, Publisher<A, F>>

extension Reader {
    /// Monadic bind for the Reader-over-Publisher stack: threads the inner value through fn, re-reading the shared environment.
    func flatMapT<A: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, Publisher<B, F>>
    ) -> Reader<Environment, Publisher<B, F>>
    where Output == Publisher<A, F>, Environment: Sendable {
        Reader<Environment, Publisher<B, F>> { env in
            self.runReader(env).flatMap(maxPublishers: 1) { a in fn(a).runReader(env) }
        }
    }
}
