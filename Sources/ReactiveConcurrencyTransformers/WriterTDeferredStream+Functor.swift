// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTDeferredStream: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>> — the log is carried INSIDE the effect.
// (Previously Writer<W, DeferredStream<A>>, which kept the log outside the effect.)

public extension DeferredStream {
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> DeferredStream<Writer<W, B>>
    where Element == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }

    static func fmapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (DeferredStream<Writer<W, Inner>>) -> DeferredStream<Writer<W, B>> {
        { $0.mapT(fn) }
    }
}

public func mapTWriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    stream.mapT(fn)
}

public func fmapTWriterDeferredStream<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<Writer<W, A>>) -> DeferredStream<Writer<W, B>> {
    { stream in mapTWriterDeferredStream(fn, stream) }
}
