// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTPublisher: the WriterT monad transformer over Publisher.
// Representation: Publisher<Writer<W, A>, F> — the log is carried INSIDE the effect.
// (Previously Writer<W, Publisher<A, F>>, which kept the log outside the effect.)

public extension Publisher {
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Publisher<Writer<W, B>, Failure>
    where Output == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }

    static func fmapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (Publisher<Writer<W, Inner>, Failure>) -> Publisher<Writer<W, B>, Failure> {
        { $0.mapT(fn) }
    }
}

public func mapTWriterPublisher<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    publisher.mapT(fn)
}

public func fmapTWriterPublisher<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<Writer<W, A>, F>) -> Publisher<Writer<W, B>, F> {
    { publisher in mapTWriterPublisher(fn, publisher) }
}
