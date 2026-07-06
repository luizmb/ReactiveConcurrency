// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Writer<w, Publisher<a, f>> -> Writer<w, Publisher<b, f>>
public func <£^> <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ writer: Writer<W, Publisher<A, F>>
) -> Writer<W, Publisher<B, F>> {
    writer.mapT(fn)
}

// (<&^>) :: Writer<w, Publisher<a, f>> -> (a -> b) -> Writer<w, Publisher<b, f>>
public func <&^> <W: Monoid, A: Sendable, B: Sendable, F: Error>(
    _ writer: Writer<W, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Writer<W, Publisher<B, F>> {
    writer.mapT(fn)
}
