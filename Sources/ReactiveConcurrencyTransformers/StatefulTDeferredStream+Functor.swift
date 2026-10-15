// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// StatefulTDeferredStream: outer = Stateful, inner = DeferredStream
// Type: Stateful<S, DeferredStream<A>>

extension Stateful {
    /// Functor map over the Stateful-over-DeferredStream stack: transforms the innermost value, leaving the Stateful and DeferredStream layers
    /// intact.
    func mapT<Inner: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (Inner) -> B
    ) -> Stateful<S, DeferredStream<B>>
    where A == DeferredStream<Inner> {
        mapStateful { stream in stream.map(fn) }
    }
}
