// SPDX-License-Identifier: Apache-2.0

/// Free-function `join`: flattens a nested `DeferredTask` into a single task.
public func join<A: Sendable>(_ nested: DeferredTask<DeferredTask<A>>) -> DeferredTask<A> {
    DeferredTask<DeferredTask<A>>.join(nested)
}

/// Free-function `void`: discards a task's success value, yielding `DeferredTask<Void>`.
public func void<A: Sendable>(_ fa: DeferredTask<A>) -> DeferredTask<Void> {
    fa.void()
}

/// Free-function `join`: flattens a nested `DeferredStream` into a single stream.
public func join<A: Sendable>(_ nested: DeferredStream<DeferredStream<A>>) -> DeferredStream<A> {
    DeferredStream<DeferredStream<A>>.join(nested)
}

/// Free-function `void`: discards a stream's element values, yielding `DeferredStream<Void>`.
public func void<A: Sendable>(_ fa: DeferredStream<A>) -> DeferredStream<Void> {
    fa.void()
}
