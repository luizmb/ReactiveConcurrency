// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// StatefulTDeferredTask: outer = Stateful, inner = DeferredTask
// Type: Stateful<S, DeferredTask<A>>

public extension Stateful {
    func mapT<Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Stateful<S, DeferredTask<B>>
    where A == DeferredTask<Inner> {
        mapStateful { task in task.map(fn) }
    }

    static func fmapT<Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> @Sendable (Stateful<S, DeferredTask<Inner>>) -> Stateful<S, DeferredTask<B>>
    where A == DeferredTask<Inner> {
        { $0.mapT(fn) }
    }
}
