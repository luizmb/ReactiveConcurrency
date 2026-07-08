// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (>>-) :: Publisher<Writer<w, a>, f> -> (a -> Publisher<Writer<w, b>, f>) -> Publisher<Writer<w, b>, f>
public func >>- <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ publisher: Publisher<Writer<W, A>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Writer<W, B>, F>
) -> Publisher<Writer<W, B>, F> {
    publisher.flatMapT(fn)
}

// (-<<) :: (a -> Publisher<Writer<w, b>, f>) -> Publisher<Writer<w, a>, f> -> Publisher<Writer<w, b>, f>
public func -<< <W: Monoid & Sendable, A: Sendable, B: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Writer<W, B>, F>,
    _ publisher: Publisher<Writer<W, A>, F>
) -> Publisher<Writer<W, B>, F> {
    publisher.flatMapT(fn)
}

// (>=>) :: (a -> Publisher<Writer<w, b>, f>) -> (b -> Publisher<Writer<w, c>, f>) -> a -> Publisher<Writer<w, c>, f>
public func >=> <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<Writer<W, B>, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<Writer<W, C>, F>
) -> @Sendable (A) -> Publisher<Writer<W, C>, F> {
    kleisliTPublisherWriter(fn1, fn2)
}

// (<=<) :: (b -> Publisher<Writer<w, c>, f>) -> (a -> Publisher<Writer<w, b>, f>) -> a -> Publisher<Writer<w, c>, f>
public func <=< <W: Monoid & Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn2: @escaping @Sendable (B) -> Publisher<Writer<W, C>, F>,
    _ fn1: @escaping @Sendable (A) -> Publisher<Writer<W, B>, F>
) -> @Sendable (A) -> Publisher<Writer<W, C>, F> {
    fn1 >=> fn2
}
