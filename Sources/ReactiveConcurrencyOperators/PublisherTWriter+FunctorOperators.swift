// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Publisher<Writer<w, a>, f> -> Publisher<Writer<w, b>, f>
public func <£^> <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    publisher.mapT(fn)
}

// (<&^>) :: Publisher<Writer<w, a>, f> -> (a -> b) -> Publisher<Writer<w, b>, f>
public func <&^> <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Writer<W, A>, F>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<Writer<W, B>, F> {
    publisher.mapT(fn)
}
