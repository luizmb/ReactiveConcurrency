// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTDeferredStream: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>>
//
// The streams are combined via the (zippy) DeferredStream applicative and the logs are combined
// via the Writer applicative — logs accumulate left-to-right inside each paired element.

/// apply for WriterT over DeferredStream.
public func applyWriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ wf: DeferredStream<Writer<W, @Sendable (A) -> B>>,
    _ wa: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    liftA2DeferredStream { f, a in Writer<W, B>.apply(f, a) }(wf, wa)
}

/// liftA2 for WriterT over DeferredStream.
public func liftA2WriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredStream<Writer<W, A>>, DeferredStream<Writer<W, B>>) -> DeferredStream<Writer<W, C>> {
    { wa, wb in
        liftA2DeferredStream { a, b in Writer<W, C>(fn(a.value, b.value), W.combine(a.log, b.log)) }(wa, wb)
    }
}

/// seqRight for WriterT over DeferredStream.
public func seqRightWriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, B>> {
    liftA2DeferredStream { a, b in a.seqRight(b) }(lhs, rhs)
}

/// seqLeft for WriterT over DeferredStream.
public func seqLeftWriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, A>> {
    liftA2DeferredStream { a, b in a.seqLeft(b) }(lhs, rhs)
}
