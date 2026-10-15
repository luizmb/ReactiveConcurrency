// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTPublisher: outer = Reader, inner = Publisher
// Type: Reader<Env, Publisher<A, F>>

extension Reader {
    /// Functor map over the Reader-over-Publisher stack: transforms the innermost value, leaving the Reader and Publisher layers intact.
    func mapT<A: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (A) -> B
    ) -> Reader<Environment, Publisher<B, F>>
    where Output == Publisher<A, F> {
        mapReader { publisher in publisher.map(fn) }
    }
}
