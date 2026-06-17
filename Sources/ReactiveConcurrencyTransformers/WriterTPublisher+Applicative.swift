import CoreFP
import DataStructure
import ReactiveConcurrency

// WriterTPublisher: outer = Writer, inner = Publisher
// Type: Writer<W, Publisher<A, F>>. Logs combine via the Monoid.

public func applyWriterPublisher<W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ wf: Writer<W, Publisher<@Sendable (A) -> B, F>>,
    _ wa: Writer<W, Publisher<A, F>>
) -> Writer<W, Publisher<B, F>> {
    Writer<W, Publisher<B, F>>(
        applyPublisher(wf.value, wa.value),
        W.combine(wf.log, wa.log)
    )
}

public func liftA2WriterPublisher<W: Monoid, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> (Writer<W, Publisher<A, F>>, Writer<W, Publisher<B, F>>) -> Writer<W, Publisher<C, F>> {
    { wa, wb in
        Writer<W, Publisher<C, F>>(
            wa.value.zip(wb.value).map { fn($0.0, $0.1) },
            W.combine(wa.log, wb.log)
        )
    }
}

public func seqRightWriterPublisher<W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Writer<W, Publisher<A, F>>,
    _ rhs: Writer<W, Publisher<B, F>>
) -> Writer<W, Publisher<B, F>> {
    Writer<W, Publisher<B, F>>(lhs.value.seqRight(rhs.value), W.combine(lhs.log, rhs.log))
}

public func seqLeftWriterPublisher<W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Writer<W, Publisher<A, F>>,
    _ rhs: Writer<W, Publisher<B, F>>
) -> Writer<W, Publisher<A, F>> {
    Writer<W, Publisher<A, F>>(lhs.value.seqLeft(rhs.value), W.combine(lhs.log, rhs.log))
}
