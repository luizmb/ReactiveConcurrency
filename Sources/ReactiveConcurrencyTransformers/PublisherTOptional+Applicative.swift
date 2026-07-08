// SPDX-License-Identifier: Apache-2.0

import ReactiveConcurrency

// PublisherTOptional: outer = Publisher, inner = Optional
// Type: Publisher<A?, F>. nil propagates.

/// Applicative liftA2 for the Publisher-over-Optional stack: runs both effects and combines their results with fn.
public func liftA2TPublisherOptional<A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<A?, F>, Publisher<B?, F>) -> Publisher<C?, F> {
    { @Sendable pa, pb in
        pa.zip(pb).map { pair -> C? in
            guard let a = pair.0, let b = pair.1 else { return nil }
            return fn(a, b)
        }
    }
}

/// Applicative apply for the Publisher-over-Optional stack (zippy).
public func applyTPublisherOptional<A: Sendable, B: Sendable, F: Error>(
    _ fns: Publisher<(@Sendable (A) -> B)?, F>,
    _ values: Publisher<A?, F>
) -> Publisher<B?, F> {
    fns.zip(values).map { pair -> B? in
        guard let f = pair.0, let a = pair.1 else { return nil }
        return f(a)
    }
}
