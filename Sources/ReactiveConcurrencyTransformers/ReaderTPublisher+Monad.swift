// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTPublisher: outer = Reader, inner = Publisher
// Type: Reader<Env, Publisher<A, F>>

public extension Reader {
    /// Monadic bind for the Reader-over-Publisher stack: threads the inner value through fn, re-reading the shared environment.
    func flatMapT<A: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, Publisher<B, F>>
    ) -> Reader<Environment, Publisher<B, F>>
    where Output == Publisher<A, F>, Environment: Sendable {
        Reader<Environment, Publisher<B, F>> { env in
            self.runReader(env).flatMap(maxPublishers: 1) { a in fn(a).runReader(env) }
        }
    }
}

/// Monadic bind (point-free) for the Reader-over-Publisher stack: threads the inner value through fn, re-reading the shared environment.
public func bindTReaderPublisher<Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ reader: Reader<Env, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}

/// Left-to-right Kleisli composition for the Reader-over-Publisher stack.
public func kleisliTReaderPublisher<Env: Sendable, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, Publisher<C, F>>
) -> @Sendable (A) -> Reader<Env, Publisher<C, F>> {
    { @Sendable a in fn1(a).flatMapT(fn2) }
}
