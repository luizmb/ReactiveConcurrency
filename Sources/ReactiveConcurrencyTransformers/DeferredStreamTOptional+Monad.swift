// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTOptional: outer = DeferredStream, inner = Optional
// Type: DeferredStream<A?>

// flatMapT :: DeferredStream<A?> -> (A -> DeferredStream<B?>) -> DeferredStream<B?>
// nil elements pass through as nil; Some(a) is replaced by all elements of fn(a)
/// Monadic bind for the DeferredStream-over-Optional stack: nil short-circuits; a present value threads through fn.
public func flatMapTDeferredStreamOptional<A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A?>,
    _ fn: @escaping @Sendable (A) -> DeferredStream<B?>
) -> DeferredStream<B?> {
    let s = stream
    return DeferredStream<B?> {
        AsyncStream<B?> { continuation in
            let task = Task { @Sendable in
                for await optA in s {
                    if let a = optA {
                        for await optB in fn(a) {
                            continuation.yield(optB)
                        }
                    } else {
                        continuation.yield(.none)
                    }
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

/// Monadic bind (point-free) for the DeferredStream-over-Optional stack: nil short-circuits; a present value threads through fn.
public func bindTDeferredStreamOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> DeferredStream<B?>
) -> @Sendable (DeferredStream<A?>) -> DeferredStream<B?> {
    { @Sendable stream in flatMapTDeferredStreamOptional(stream, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
/// Left-to-right Kleisli composition for the DeferredStream-over-Optional stack.
public func kleisliTDeferredStreamOptional<A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> DeferredStream<B?>,
    _ fn2: @escaping @Sendable (B) -> DeferredStream<C?>
) -> @Sendable (A) -> DeferredStream<C?> {
    { @Sendable a in flatMapTDeferredStreamOptional(fn1(a), fn2) }
}
