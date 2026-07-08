// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTWriter: the WriterT monad transformer over Publisher.
// Representation: Publisher<Writer<W, A>, F>
//
// flatMapT flattens sequentially (flatMap maxPublishers: 1 — lawful ordered bind) and combines
// each element's log with the continuation's log INSIDE the effect (w1 <> w2). The previous
// shape discarded the continuation's log.

public extension Publisher {
    /// Monadic bind for the Publisher-over-Writer stack: threads the value through fn and combines the accumulated logs via the Monoid.
    func flatMapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> Publisher<Writer<W, B>, Failure>
    ) -> Publisher<Writer<W, B>, Failure>
    where Output == Writer<W, Inner> {
        flatMap(maxPublishers: 1) { w1 in
            fn(w1.value).map { w2 in Writer<W, B>(w2.value, W.combine(w1.log, w2.log)) }
        }
    }

    /// Monadic bind (point-free) for the Publisher-over-Writer stack: threads the value through fn and combines the accumulated logs via the Monoid.
    static func bindT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> Publisher<Writer<W, B>, Failure>
    ) -> @Sendable (Publisher<Writer<W, Inner>, Failure>) -> Publisher<Writer<W, B>, Failure> {
        { $0.flatMapT(fn) }
    }
}

/// Left-to-right Kleisli composition for the Publisher-over-Writer stack.
public func kleisliTPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<Writer<W, B>, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<Writer<W, C>, F>
) -> @Sendable (A) -> Publisher<Writer<W, C>, F> {
    { @Sendable a in fn1(a).flatMapT(fn2) }
}
