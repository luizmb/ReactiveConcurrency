import CoreFPOperators
import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers

// (<*>) :: DeferredStream<Either<l,a->b>> -> DeferredStream<Either<l,a>> -> DeferredStream<Either<l,b>>
public func <*> <L: Sendable, A: Sendable, B: Sendable>(
    _ fns: DeferredStream<Either<L, @Sendable (A) -> B>>,
    _ values: DeferredStream<Either<L, A>>
) -> DeferredStream<Either<L, B>> {
    applyTDeferredStreamEither(fns, values)
}
