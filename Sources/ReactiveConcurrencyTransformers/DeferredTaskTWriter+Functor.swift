// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredTaskTWriter: the WriterT monad transformer over DeferredTask.
// Representation: DeferredTask<Writer<W, A>> — the log is carried INSIDE the effect.
// (Previously modelled as Writer<W, DeferredTask<A>>, which kept the log outside the
// effect and made bind unable to combine the continuation's log — see +Monad.)

public extension DeferredTask {
    /// Functor map over the DeferredTask-over-Writer stack: transforms the innermost value, leaving the DeferredTask and Writer layers intact.
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> DeferredTask<Writer<W, B>>
    where Success == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }

    /// Functor map (point-free) for the DeferredTask-over-Writer stack: transforms the innermost value, leaving the DeferredTask and Writer layers
    /// intact.
    static func fmapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (DeferredTask<Writer<W, Inner>>) -> DeferredTask<Writer<W, B>> {
        { $0.mapT(fn) }
    }
}
