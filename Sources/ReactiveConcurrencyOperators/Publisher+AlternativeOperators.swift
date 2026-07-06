// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<|>) :: Publisher a e -> Publisher a e -> Publisher a e
// Concatenation: all of lhs, then all of rhs (only if lhs finished without failing).
public func <|> <A: Sendable, E: Error>(
    _ lhs: Publisher<A, E>,
    _ rhs: @autoclosure () -> Publisher<A, E>
) -> Publisher<A, E> {
    Publisher<A, E>.alt(lhs, rhs())
}
