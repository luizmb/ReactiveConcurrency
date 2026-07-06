// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTResult: outer = Publisher, inner = Result
// Type: Publisher<Result<A, E>, F>  — ExceptT e over a typed-failure Publisher.
// Note the two error channels: the inner Result.failure(e) short-circuits the transformer,
// while the outer Publisher Failure F terminates the stream.

public func mapTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B,
    _ publisher: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    publisher.map { result in result.map(fn) }
}

public func fmapTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (Publisher<Result<A, E>, F>) -> Publisher<Result<B, E>, F> {
    { @Sendable publisher in mapTPublisherResult(fn, publisher) }
}
