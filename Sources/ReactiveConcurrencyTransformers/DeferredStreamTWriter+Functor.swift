// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredStreamTWriter: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>> — the log is carried INSIDE the effect.
// (Previously Writer<W, DeferredStream<A>>, which kept the log outside the effect.)

extension DeferredStream {
    /// Functor map over the DeferredStream-over-Writer stack: transforms the innermost value, leaving the DeferredStream and Writer layers intact.
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> DeferredStream<Writer<W, B>>
    where Element == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }
}
