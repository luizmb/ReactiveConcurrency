// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTArray: outer = DeferredStream, inner = Array
// Type: DeferredStream<[A]>  — Haskell: ListT DeferredStream

/// Functor map over the DeferredStream-over-Array stack: transforms the innermost value, leaving the DeferredStream and Array layers intact.
public func mapTDeferredStreamArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<[A]>
) -> DeferredStream<[B]> {
    stream.map { arr in arr.map(fn) }
}

/// intact.

/// Functor map (point-free) for the DeferredStream-over-Array stack: transforms the innermost value, leaving the DeferredStream and Array layers
public func fmapTDeferredStreamArray<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<[A]>) -> DeferredStream<[B]> {
    { @Sendable stream in mapTDeferredStreamArray(fn, stream) }
}
