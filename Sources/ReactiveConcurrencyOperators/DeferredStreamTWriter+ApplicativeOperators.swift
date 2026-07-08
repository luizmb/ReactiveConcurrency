// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<Writer<w, (a -> b)>> -> DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>>

/// Applicative apply for the DeferredStream-over-Writer stack.
public func <*> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ wf: DeferredStream<Writer<W, @Sendable (A) -> B>>,
    _ wa: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    applyDeferredStreamWriter(wf, wa)
}

// (*>) :: DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>> -> DeferredStream<Writer<w, b>>

/// Sequences two effects, keeping the right result for the DeferredStream-over-Writer stack. Operator form of `seqRight`.
public func *> <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, B>> {
    seqRightDeferredStreamWriter(lhs, rhs)
}

// (<*) :: DeferredStream<Writer<w, a>> -> DeferredStream<Writer<w, b>> -> DeferredStream<Writer<w, a>>

/// Sequences two effects, keeping the left result for the DeferredStream-over-Writer stack. Operator form of `seqLeft`.
public func <* <W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ lhs: DeferredStream<Writer<W, A>>,
    _ rhs: DeferredStream<Writer<W, B>>
) -> DeferredStream<Writer<W, A>> {
    seqLeftDeferredStreamWriter(lhs, rhs)
}
