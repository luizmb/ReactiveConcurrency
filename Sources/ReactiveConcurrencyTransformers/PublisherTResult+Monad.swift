// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTResult: outer = Publisher, inner = Result
// Type: Publisher<Result<A, E>, F>

// flatMapT: inner .failure short-circuits (emitted as failure); .success(a) proceeds through fn.
// Sequential (maxPublishers: 1) preserves emission order.
public func flatMapTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ publisher: Publisher<Result<A, E>, F>,
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> Publisher<Result<B, E>, F> {
    publisher.flatMap(maxPublishers: 1) { result in
        switch result {
        case let .success(a): fn(a)
        case let .failure(e): Publisher<Result<B, E>, F>.just(.failure(e))
        }
    }
}

public func bindTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>
) -> @Sendable (Publisher<Result<A, E>, F>) -> Publisher<Result<B, E>, F> {
    { @Sendable publisher in flatMapTPublisherResult(publisher, fn) }
}

// Kleisli composition (left-to-right): the named function >=>/<=< delegate to.
public func kleisliTPublisherResult<A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Publisher<Result<B, E>, F>,
    _ fn2: @escaping @Sendable (B) -> Publisher<Result<C, E>, F>
) -> @Sendable (A) -> Publisher<Result<C, E>, F> {
    { @Sendable a in flatMapTPublisherResult(fn1(a), fn2) }
}
