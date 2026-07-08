// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTWriter: the WriterT monad transformer over Publisher.
// Representation: Publisher<Writer<W, A>, F>
//
// The publishers are combined via Publisher.zip and the logs via the Writer applicative —
// so logs accumulate left-to-right inside each paired element.

/// Applicative apply for the Publisher-over-Writer stack (zippy); logs are combined via the Monoid.
public func applyPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ wf: Publisher<Writer<W, @Sendable (A) -> B>, F>,
    _ wa: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    wf.zip(wa).map { Writer<W, B>.apply($0.0, $0.1) }
}

/// Applicative liftA2 for the Publisher-over-Writer stack: runs both effects and combines their results with fn; logs are combined via the Monoid.
public func liftA2PublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Writer<W, A>, F>, Publisher<Writer<W, B>, F>) -> Publisher<Writer<W, C>, F> {
    { wa, wb in
        wa.zip(wb).map { Writer<W, C>(fn($0.0.value, $0.1.value), W.combine($0.0.log, $0.1.log)) }
    }
}

/// Applicative seqRight for the Publisher-over-Writer stack: sequences both effects, keeps the right result; logs are combined via the Monoid.
public func seqRightPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Publisher<Writer<W, A>, F>,
    _ rhs: Publisher<Writer<W, B>, F>
) -> Publisher<Writer<W, B>, F> {
    lhs.zip(rhs).map { $0.0.seqRight($0.1) }
}

/// Applicative seqLeft for the Publisher-over-Writer stack: sequences both effects, keeps the left result; logs are combined via the Monoid.
public func seqLeftPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Publisher<Writer<W, A>, F>,
    _ rhs: Publisher<Writer<W, B>, F>
) -> Publisher<Writer<W, A>, F> {
    lhs.zip(rhs).map { $0.0.seqLeft($0.1) }
}
