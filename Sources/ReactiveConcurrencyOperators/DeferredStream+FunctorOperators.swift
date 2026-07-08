// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import ReactiveConcurrency

// (<£>) :: (a -> b) -> DeferredStream a -> DeferredStream b

/// Functor map (function on the left). Operator form of `map`.
public func <£> <A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ stream: DeferredStream<A>
) -> DeferredStream<B> {
    stream.map(fn)
}

// (<&>) :: DeferredStream a -> (a -> b) -> DeferredStream b

/// Functor map (container on the left). Operator form of `map`.
public func <&> <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A>,
    _ fn: @escaping @Sendable (A) -> B
) -> DeferredStream<B> {
    stream.map(fn)
}

// (£>) :: DeferredStream a -> b -> DeferredStream b

/// Replaces every value with a constant (container on the left). Operator form of `replace`.
public func £> <A: Sendable, B: Sendable>(
    _ stream: DeferredStream<A>,
    _ value: B
) -> DeferredStream<B> {
    stream.replace(value)
}

// (<£) :: b -> DeferredStream a -> DeferredStream b

/// Replaces every value with a constant (constant on the left). Operator form of `replace`.
public func <£ <A: Sendable, B: Sendable>(
    _ value: B,
    _ stream: DeferredStream<A>
) -> DeferredStream<B> {
    stream £> value
}
