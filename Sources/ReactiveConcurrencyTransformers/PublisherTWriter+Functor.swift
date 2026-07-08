// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// PublisherTWriter: the WriterT monad transformer over Publisher.
// Representation: Publisher<Writer<W, A>, F> — the log is carried INSIDE the effect.
// (Previously Writer<W, Publisher<A, F>>, which kept the log outside the effect.)

public extension Publisher {
    /// Functor map over the Publisher-over-Writer stack: transforms the innermost value, leaving the Publisher and Writer layers intact.
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Publisher<Writer<W, B>, Failure>
    where Output == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }

    /// Functor map (point-free) for the Publisher-over-Writer stack: transforms the innermost value, leaving the Publisher and Writer layers intact.
    static func fmapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (Publisher<Writer<W, Inner>, Failure>) -> Publisher<Writer<W, B>, Failure> {
        { $0.mapT(fn) }
    }
}

/// Functor map over the Publisher-over-Writer stack: transforms the innermost value, leaving the Publisher and Writer layers intact.
public func mapTPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    publisher.mapT(fn)
}

/// Functor map (point-free) for the Publisher-over-Writer stack: transforms the innermost value, leaving the Publisher and Writer layers intact.
public func fmapTPublisherWriter<W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<Writer<W, A>, F>) -> Publisher<Writer<W, B>, F> {
    { publisher in mapTPublisherWriter(fn, publisher) }
}
