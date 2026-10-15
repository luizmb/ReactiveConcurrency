// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredStreamTWriter: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>>
//
// flatMapT concatMaps the stream and combines each element's log with the continuation's log
// INSIDE the effect (w1 <> w2). The previous shape discarded the continuation's log.

extension DeferredStream {
    /// Monadic bind for the DeferredStream-over-Writer stack: threads the value through fn and combines the accumulated logs via the Monoid.
    func flatMapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> DeferredStream<Writer<W, B>>
    ) -> DeferredStream<Writer<W, B>>
    where Element == Writer<W, Inner> {
        flatMap { w1 in
            fn(w1.value).map { w2 in Writer<W, B>(w2.value, W.combine(w1.log, w2.log)) }
        }
    }
}
