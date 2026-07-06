// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTResult: outer = Publisher, inner = Result
// Type: Publisher<Result<A, E>, F>. First inner .failure wins.

public func liftA2TPublisherResult<A: Sendable, B: Sendable, C: Sendable, E: Error & Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<Result<A, E>, F>, Publisher<Result<B, E>, F>) -> Publisher<Result<C, E>, F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair -> Result<C, E> in
            switch (pair.0, pair.1) {
            case let (.success(a), .success(b)): .success(fn(a, b))
            case let (.failure(e), _): .failure(e)
            case let (_, .failure(e)): .failure(e)
            }
        }
    }
}

public func applyTPublisherResult<A: Sendable, B: Sendable, E: Error & Sendable, F: Error>(
    _ fns: Publisher<Result<@Sendable (A) -> B, E>, F>,
    _ values: Publisher<Result<A, E>, F>
) -> Publisher<Result<B, E>, F> {
    fns.zip(values).map { pair -> Result<B, E> in
        switch (pair.0, pair.1) {
        case let (.success(f), .success(a)): .success(f(a))
        case let (.failure(e), _): .failure(e)
        case let (_, .failure(e)): .failure(e)
        }
    }
}
