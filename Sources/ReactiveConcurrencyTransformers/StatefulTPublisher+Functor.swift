// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// StatefulTPublisher: outer = Stateful, inner = Publisher
// Type: Stateful<S, Publisher<A, F>>

public extension Stateful {
    func mapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Stateful<S, Publisher<B, F>>
    where A == Publisher<Inner, F> {
        mapStateful { publisher in publisher.map(fn) }
    }

    static func fmapT<Inner: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (Stateful<S, Publisher<Inner, F>>) -> Stateful<S, Publisher<B, F>>
    where A == Publisher<Inner, F> {
        { $0.mapT(fn) }
    }
}
