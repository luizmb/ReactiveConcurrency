// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// StatefulTPublisher: outer = Stateful, inner = Publisher
// Type: Stateful<S, Publisher<A, F>>

extension Stateful {
    /// Functor map over the Stateful-over-Publisher stack: transforms the innermost value, leaving the Stateful and Publisher layers intact.
    func mapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Stateful<S, Publisher<B, F>>
    where A == Publisher<Inner, F> {
        mapStateful { publisher in publisher.map(fn) }
    }
}
