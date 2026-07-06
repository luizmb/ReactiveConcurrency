// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// WriterTPublisher: outer = Writer, inner = Publisher
// Type: Writer<W, Publisher<A, F>>
// flatMapT keeps the outer log; the inner publisher is flattened sequentially.

public extension Writer {
    func flatMapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> Writer<W, Publisher<B, F>>
    ) -> Writer<W, Publisher<B, F>>
    where A == Publisher<Inner, F> {
        let outerLog = log
        let innerStream = value.flatMap(maxPublishers: 1) { a -> Publisher<B, F> in fn(a).value }
        return Writer<W, Publisher<B, F>>(innerStream, outerLog)
    }
}
