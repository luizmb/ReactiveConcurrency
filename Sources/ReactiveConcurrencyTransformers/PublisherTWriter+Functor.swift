// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTWriter: the WriterT monad transformer over Publisher.
// Representation: Publisher<Writer<W, A>, F> — the log is carried INSIDE the effect.
// (Previously Writer<W, Publisher<A, F>>, which kept the log outside the effect.)

extension Publisher {
    /// Functor map over the Publisher-over-Writer stack: transforms the innermost value, leaving the Publisher and Writer layers intact.
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Publisher<Writer<W, B>, Failure>
    where Output == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }
}
