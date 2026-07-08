// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<Writer<w, (a -> b)>> -> DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>>
public func <*> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ wf: DeferredStream<Writer<W, @Sendable (A) -> B>>,
    _ wa: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    applyWriterDeferredStream(wf, wa)
}

// (*>) :: DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>> -> DeferredStream<Writer<w, b>>
public func *> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, B>> {
    seqRightWriterDeferredStream(lhs, rhs)
}

// (<*) :: DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>> -> DeferredStream<Writer<w, a>>
public func <* <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, A>> {
    seqLeftWriterDeferredStream(lhs, rhs)
}
