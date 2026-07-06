// SPDX-License-Identifier: Apache-2.0

import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTPublisher: outer = Writer, inner = Publisher
// Type: Writer<W, Publisher<A, F>>

public extension Writer {
    func mapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Writer<W, Publisher<B, F>>
    where A == Publisher<Inner, F> {
        Writer<W, Publisher<B, F>>(value.map(fn), log)
    }

    static func fmapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (Writer<W, Publisher<Inner, F>>) -> Writer<W, Publisher<B, F>> {
        { $0.mapT(fn) }
    }
}

public func mapTWriterPublisher<W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ writer: Writer<W, Publisher<A, F>>
) -> Writer<W, Publisher<B, F>> {
    Writer<W, Publisher<B, F>>(writer.value.map(fn), writer.log)
}

public func fmapTWriterPublisher<W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Writer<W, Publisher<A, F>>) -> Writer<W, Publisher<B, F>> {
    { writer in mapTWriterPublisher(fn, writer) }
}
