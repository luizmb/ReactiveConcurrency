// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredTask<Writer<w, (a -> b)>> -> DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>>
public func <*> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ wf: DeferredTask<Writer<W, @Sendable (A) -> B>>,
    _ wa: DeferredTask<Writer<W, A>>
) -> DeferredTask<Writer<W, B>> {
    applyDeferredTaskWriter(wf, wa)
}

// (*>) :: DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>> -> DeferredTask<Writer<w, b>>
public func *> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<Writer<W, A>>,
    _ rhs: DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, B>> {
    seqRightDeferredTaskWriter(lhs, rhs)
}

// (<*) :: DeferredTask<Writer<w, a>> -> DeferredTask<Writer<w, b>> -> DeferredTask<Writer<w, a>>
public func <* <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredTask<Writer<W, A>>,
    _ rhs: DeferredTask<Writer<W, B>>
) -> DeferredTask<Writer<W, A>> {
    seqLeftDeferredTaskWriter(lhs, rhs)
}
