import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import CoreFPOperators

// (<|>) :: DeferredTask<A?> -> DeferredTask<A?> -> DeferredTask<A?>
public func <|> <A: Sendable>(
    _ lhs: DeferredTask<A?>,
    _ rhs: @autoclosure () -> DeferredTask<A?>
) -> DeferredTask<A?> {
    altDeferredTaskOptional(lhs, rhs())
}
