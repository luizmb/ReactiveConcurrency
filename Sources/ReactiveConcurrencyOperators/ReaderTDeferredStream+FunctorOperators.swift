import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import CoreFPOperators
import DataStructure

// (<£^>) :: (a -> b) -> Reader<env, DeferredStream<a>> -> Reader<env, DeferredStream<b>>
public func <£^> <Env, A: Sendable, B: Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ reader: Reader<Env, DeferredStream<A>>
) -> Reader<Env, DeferredStream<B>> {
    reader.mapT(fn)
}

// (<&^>) :: Reader<env, DeferredStream<a>> -> (a -> b) -> Reader<env, DeferredStream<b>>
public func <&^> <Env, A: Sendable, B: Sendable>(
    _ reader: Reader<Env, DeferredStream<A>>,
    _ fn: @escaping @Sendable (A) -> B
) -> Reader<Env, DeferredStream<B>> {
    reader.mapT(fn)
}
