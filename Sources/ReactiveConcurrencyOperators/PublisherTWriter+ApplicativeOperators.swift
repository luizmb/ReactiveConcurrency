// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Publisher<Writer<w, a->b>, f> -> Publisher<Writer<w, a>, f> -> Publisher<Writer<w, b>, f>
public func <*> <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ wf: Publisher<Writer<W, @Sendable (A) -> B>, F>,
    _ wa: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    applyPublisherWriter(wf, wa)
}

// (*>) :: Publisher<Writer<w, a>, f> -> Publisher<Writer<w, b>, f> -> Publisher<Writer<w, b>, f>
public func *> <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Publisher<Writer<W, A>, F>,
    _ rhs: Publisher<Writer<W, B>, F>
) -> Publisher<Writer<W, B>, F> {
    seqRightPublisherWriter(lhs, rhs)
}

// (<*) :: Publisher<Writer<w, a>, f> -> Publisher<Writer<w, b>, f> -> Publisher<Writer<w, a>, f>
public func <* <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Publisher<Writer<W, A>, F>,
    _ rhs: Publisher<Writer<W, B>, F>
) -> Publisher<Writer<W, A>, F> {
    seqLeftPublisherWriter(lhs, rhs)
}
