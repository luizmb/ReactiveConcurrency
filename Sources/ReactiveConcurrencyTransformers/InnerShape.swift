// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// Inner-shape protocols for ReactiveConcurrency's effect types, completing FP's `ArrayLike`,
// `OptionalLike`, … (see `ArrayLike` in CoreFP for the rationale). Swift has no parameterized
// extensions, so `reader.readerT` / `stateful.statefulT` constrain the inner layer through these.
// Each protocol has exactly one conformer and its requirement is the identity on it (O(1), no copies).

/// The shape of `DeferredTask<Success>`, used to lift `F<DeferredTask<A>>` into an `FTDeferredTask` stack.
public protocol DeferredTaskLike<Success>: SendableMetatype {
    /// The task's result.
    associatedtype Success: Sendable

    /// Views a value as `DeferredTask<Success>`; the identity for `DeferredTask`.
    static func asDeferredTask(_ value: Self) -> DeferredTask<Success>
}

extension DeferredTask: DeferredTaskLike {
    /// Identity: a task already is `DeferredTask<Success>`.
    public static func asDeferredTask(_ value: DeferredTask<Success>) -> DeferredTask<Success> {
        value
    }
}

/// The shape of `DeferredStream<Element>`, used to lift `F<DeferredStream<A>>` into an `FTDeferredStream` stack.
public protocol DeferredStreamLike<Element>: SendableMetatype {
    /// The stream element.
    associatedtype Element: Sendable

    /// Views a value as `DeferredStream<Element>`; the identity for `DeferredStream`.
    static func asDeferredStream(_ value: Self) -> DeferredStream<Element>
}

extension DeferredStream: DeferredStreamLike {
    /// Identity: a stream already is `DeferredStream<Element>`.
    public static func asDeferredStream(_ value: DeferredStream<Element>) -> DeferredStream<Element> {
        value
    }
}

/// The shape of `Publisher<Output, Failure>`, used to lift `F<Publisher<A, E>>` into an `FTPublisher` stack.
public protocol PublisherLike<Output, Failure>: SendableMetatype {
    /// The published value.
    associatedtype Output: Sendable
    /// The failure.
    associatedtype Failure: Error

    /// Views a value as `Publisher<Output, Failure>`; the identity for `Publisher`.
    static func asPublisher(_ value: Self) -> Publisher<Output, Failure>
}

extension Publisher: PublisherLike {
    /// Identity: a publisher already is `Publisher<Output, Failure>`.
    public static func asPublisher(_ value: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        value
    }
}
