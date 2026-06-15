import CoreFPOperators
import ReactiveConcurrency

// (>>-) :: ZIOKleisli<i, env, a, e> -> (a -> ZIOKleisli<i, env, b, e>) -> ZIOKleisli<i, env, b, e>
public func >>- <I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ k: ZIOKleisli<I, Env, A, E>,
    _ fn: @escaping @Sendable (A) -> ZIOKleisli<I, Env, B, E>
) -> ZIOKleisli<I, Env, B, E> {
    k.flatMap(fn)
}

// (-<<) :: (a -> ZIOKleisli<i, env, b, e>) -> ZIOKleisli<i, env, a, e> -> ZIOKleisli<i, env, b, e>
public func -<< <I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> ZIOKleisli<I, Env, B, E>,
    _ k: ZIOKleisli<I, Env, A, E>
) -> ZIOKleisli<I, Env, B, E> {
    k >>- fn
}

// (>=>) :: ZIOKleisli<i, env, a, e> -> ZIOKleisli<a, env, b, e> -> ZIOKleisli<i, env, b, e>
public func >=> <I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ k1: ZIOKleisli<I, Env, A, E>,
    _ k2: ZIOKleisli<A, Env, B, E>
) -> ZIOKleisli<I, Env, B, E> {
    k1.andThen(k2)
}

// (<=<) :: ZIOKleisli<a, env, b, e> -> ZIOKleisli<i, env, a, e> -> ZIOKleisli<i, env, b, e>
public func <=< <I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ k2: ZIOKleisli<A, Env, B, E>,
    _ k1: ZIOKleisli<I, Env, A, E>
) -> ZIOKleisli<I, Env, B, E> {
    k1.andThen(k2)
}
