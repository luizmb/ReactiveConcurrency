// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// ReaderTPublisher: outer = Reader, inner = Publisher
// Type: Reader<Env, Publisher<A, F>>

/// Applicative apply for the Reader-over-Publisher stack.
public func applyReaderPublisher<Env, A: Sendable, B: Sendable, F: Error>(
    _ rf: Reader<Env, Publisher<@Sendable (A) -> B, F>>,
    _ ra: Reader<Env, Publisher<A, F>>
) -> Reader<Env, Publisher<B, F>> {
    Reader { env in applyPublisher(rf(env), ra(env)) }
}

/// Applicative liftA2 for the Reader-over-Publisher stack: runs both effects and combines their results with fn.
public func liftA2ReaderPublisher<Env, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Reader<Env, Publisher<A, F>>, Reader<Env, Publisher<B, F>>) -> Reader<Env, Publisher<C, F>> {
    { ra, rb in
        Reader { env in ra(env).zip(rb(env)).map { fn($0.0, $0.1) } }
    }
}

/// Applicative seqRight for the Reader-over-Publisher stack: sequences both effects, keeps the right result.
public func seqRightReaderPublisher<Env, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Reader<Env, Publisher<A, F>>,
    _ rhs: Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    Reader { env in lhs(env).seqRight(rhs(env)) }
}

/// Applicative seqLeft for the Reader-over-Publisher stack: sequences both effects, keeps the left result.
public func seqLeftReaderPublisher<Env, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Reader<Env, Publisher<A, F>>,
    _ rhs: Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<A, F>> {
    Reader { env in lhs(env).seqLeft(rhs(env)) }
}
