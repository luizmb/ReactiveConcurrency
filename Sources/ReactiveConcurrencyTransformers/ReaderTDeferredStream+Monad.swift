// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTDeferredStream: outer = Reader, inner = DeferredStream
// Type: Reader<Env, DeferredStream<A>>

public extension Reader {
    /// Monadic bind for the Reader-over-DeferredStream stack: threads the inner value through fn, re-reading the shared environment.
    func flatMapT<A: Sendable, B: Sendable>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, DeferredStream<B>>
    ) -> Reader<Environment, DeferredStream<B>>
    where Output == DeferredStream<A>, Environment: Sendable {
        Reader<Environment, DeferredStream<B>> { env in
            self.runReader(env).flatMap { a in fn(a).runReader(env) }
        }
    }
}

/// Monadic bind (point-free) for the Reader-over-DeferredStream stack: threads the inner value through fn, re-reading the shared environment.
public func bindTReaderDeferredStream<Env: Sendable, A: Sendable, B: Sendable>(
    _ reader: Reader<Env, DeferredStream<A>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>
) -> Reader<Env, DeferredStream<B>> {
    reader.flatMapT(fn)
}

/// Left-to-right Kleisli composition for the Reader-over-DeferredStream stack.
public func kleisliTReaderDeferredStream<Env: Sendable, A: Sendable, B: Sendable, C: Sendable>(
    _ fn1: @escaping @Sendable (A) -> Reader<Env, DeferredStream<B>>,
    _ fn2: @escaping @Sendable (B) -> Reader<Env, DeferredStream<C>>
) -> @Sendable (A) -> Reader<Env, DeferredStream<C>> {
    { @Sendable a in fn1(a).flatMapT(fn2) }
}
