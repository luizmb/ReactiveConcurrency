// SPDX-License-Identifier: Apache-2.0

public func join<A: Sendable>(_ nested: DeferredTask<DeferredTask<A>>) -> DeferredTask<A> {
    DeferredTask<DeferredTask<A>>.join(nested)
}

public func void<A: Sendable>(_ fa: DeferredTask<A>) -> DeferredTask<Void> {
    fa.void()
}

public func join<A: Sendable>(_ nested: DeferredStream<DeferredStream<A>>) -> DeferredStream<A> {
    DeferredStream<DeferredStream<A>>.join(nested)
}

public func void<A: Sendable>(_ fa: DeferredStream<A>) -> DeferredStream<Void> {
    fa.void()
}
