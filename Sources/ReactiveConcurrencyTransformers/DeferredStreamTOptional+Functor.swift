// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// DeferredStreamTOptional: outer = DeferredStream, inner = Optional
// Type: DeferredStream<A?>  — Haskell: MaybeT DeferredStream

// mapT maps inside the Optional, leaving the DeferredStream layer intact
/// Functor map over the DeferredStream-over-Optional stack: transforms the innermost value, leaving the DeferredStream and Optional layers intact.
public func mapTDeferredStreamOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<A?>
) -> DeferredStream<B?> {
    stream.map { optA in optA.map(fn) }
}

/// Functor map (point-free) for the DeferredStream-over-Optional stack: transforms the innermost value, leaving the DeferredStream and Optional
/// layers intact.
public func fmapTDeferredStreamOptional<A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (DeferredStream<A?>) -> DeferredStream<B?> {
    { @Sendable stream in mapTDeferredStreamOptional(fn, stream) }
}
