// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredStreamTWriter: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>>
//
// flatMapT concatMaps the stream and combines each element's log with the continuation's log
// INSIDE the effect (w1 <> w2). The previous shape discarded the continuation's log.

public extension DeferredStream {
    func flatMapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> DeferredStream<Writer<W, B>>
    ) -> DeferredStream<Writer<W, B>>
    where Element == Writer<W, Inner> {
        flatMap { w1 in
            fn(w1.value).map { w2 in Writer<W, B>(w2.value, W.combine(w1.log, w2.log)) }
        }
    }

    static func bindT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> DeferredStream<Writer<W, B>>
    ) -> @Sendable (DeferredStream<Writer<W, Inner>>) -> DeferredStream<Writer<W, B>> {
        { $0.flatMapT(fn) }
    }
}

public func kleisliTDeferredStreamWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<Writer<W, B>>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<Writer<W, C>>
) -> @Sendable (A) -> DeferredStream<Writer<W, C>> {
    { @Sendable a in fn1(a).flatMapT(fn2) }
}
