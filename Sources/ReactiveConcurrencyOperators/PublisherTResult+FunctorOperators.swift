// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<£^>) :: (a -> b) -> Publisher<Result<a,e>, f> -> Publisher<Result<b,e>, f>
public func <£^> <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    mapTPublisherResult(fn, publisher)
}

// (<&^>) :: Publisher<Result<a,e>, f> -> (a -> b) -> Publisher<Result<b,e>, f>
public func <&^> <A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ publisher: Publisher<Result<A, E>, F>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<Result<B, E>, F> {
    mapTPublisherResult(fn, publisher)
}
