// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// DeferredStreamTWriter: the WriterT monad transformer over DeferredStream.
// Representation: DeferredStream<Writer<W, A>> — the log is carried INSIDE the effect.
// (Previously Writer<W, DeferredStream<A>>, which kept the log outside the effect.)

public extension DeferredStream {
    /// Functor map over the DeferredStream-over-Writer stack: transforms the innermost value, leaving the DeferredStream and Writer layers intact.
    func mapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> DeferredStream<Writer<W, B>>
    where Element == Writer<W, Inner> {
        map { $0.mapWriter(fn) }
    }

    /// Functor map (point-free) for the DeferredStream-over-Writer stack: transforms the innermost value, leaving the DeferredStream and Writer
    /// layers intact.
    static func fmapT<W: Monoid & Sendable, Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (DeferredStream<Writer<W, Inner>>) -> DeferredStream<Writer<W, B>> {
        { $0.mapT(fn) }
    }
}

/// Functor map over the DeferredStream-over-Writer stack: transforms the innermost value, leaving the DeferredStream and Writer layers intact.
public func mapTDeferredStreamWriter<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<Writer<W, A>>
) -> DeferredStream<Writer<W, B>> {
    stream.mapT(fn)
}

/// Functor map (point-free) for the DeferredStream-over-Writer stack: transforms the innermost value, leaving the DeferredStream and Writer layers
/// intact.
public func fmapTDeferredStreamWriter<W: Monoid & Sendable, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<Writer<W, A>>) -> DeferredStream<Writer<W, B>> {
    { stream in mapTDeferredStreamWriter(fn, stream) }
}
