// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: Writer<w, Publisher<a->b, f>> -> Writer<w, Publisher<a, f>> -> Writer<w, Publisher<b, f>>
public func <*> <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ wf: Writer<W, Publisher<@Sendable (A) -> B, F>>,
    _ wa: Writer<W, Publisher<A, F>>
) -> Writer<W, Publisher<B, F>> {
    applyWriterPublisher(wf, wa)
}

// (*>) :: Writer<w, Publisher<a, f>> -> Writer<w, Publisher<b, f>> -> Writer<w, Publisher<b, f>>
public func *> <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Writer<W, Publisher<A, F>>,
    _ rhs: Writer<W, Publisher<B, F>>
) -> Writer<W, Publisher<B, F>> {
    seqRightWriterPublisher(lhs, rhs)
}

// (<*) :: Writer<w, Publisher<a, f>> -> Writer<w, Publisher<b, f>> -> Writer<w, Publisher<a, f>>
public func <* <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Writer<W, Publisher<A, F>>,
    _ rhs: Writer<W, Publisher<B, F>>
) -> Writer<W, Publisher<A, F>> {
    seqLeftWriterPublisher(lhs, rhs)
}
