// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTDeferredTask: the WriterT monad transformer over DeferredTask.
// Representation: DeferredTask<Writer<W, A>>
//
// The effects run sequentially (DeferredTask applicative) and the logs are combined via the
// Writer applicative — so logs accumulate left-to-right inside the effect.

/// apply for WriterT over DeferredTask.
public func applyWriterDeferredTask<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ wf: DeferredTask<Writer<W, @Sendable (A) -> B>>,
    _ wa: DeferredTask<Writer<W, A>>
) -> DeferredTask<Writer<W, B>> {
    liftA2DeferredTask { f, a in Writer<W, B>.apply(f, a) }(wf, wa)
}

/// liftA2 for WriterT over DeferredTask.
public func liftA2WriterDeferredTask<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (DeferredTask<Writer<W, A>>, DeferredTask<Writer<W, B>>) -> DeferredTask<Writer<W, C>> {
    { wa, wb in
        liftA2DeferredTask { a, b in Writer<W, C>(fn(a.value, b.value), W.combine(a.log, b.log)) }(wa, wb)
    }
}

/// seqRight for WriterT over DeferredTask.
public func seqRightWriterDeferredTask<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<Writer<W, A>>,
    _ rhs: DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, B>> {
    liftA2DeferredTask { a, b in a.seqRight(b) }(lhs, rhs)
}

/// seqLeft for WriterT over DeferredTask.
public func seqLeftWriterDeferredTask<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<Writer<W, A>>,
    _ rhs: DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, A>> {
    liftA2DeferredTask { a, b in a.seqLeft(b) }(lhs, rhs)
}
