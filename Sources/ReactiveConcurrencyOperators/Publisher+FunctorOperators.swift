// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<£>) :: (a -> b) -> Publisher a e -> Publisher b e
public func <£> <A: Sendable, B: Sendable, E: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<A, E>
) -> Publisher<B, E> {
    publisher.map(fn)
}

// (<&>) :: Publisher a e -> (a -> b) -> Publisher b e
public func <&> <A: Sendable, B: Sendable, E: Error>(
    _ publisher: Publisher<A, E>,
    _ fn: @escaping @Sendable (A) -> B
) -> Publisher<B, E> {
    publisher.map(fn)
}

// (£>) :: Publisher a e -> b -> Publisher b e
public func £> <A: Sendable, B: Sendable, E: Error>(
    _ publisher: Publisher<A, E>,
    _ value: B
) -> Publisher<B, E> {
    publisher.replace(value)
}

// (<£) :: b -> Publisher a e -> Publisher b e
public func <£ <A: Sendable, B: Sendable, E: Error>(
    _ value: B,
    _ publisher: Publisher<A, E>
) -> Publisher<B, E> {
    publisher £> value
}
