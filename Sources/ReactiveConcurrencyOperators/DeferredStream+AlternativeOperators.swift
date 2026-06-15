import CoreFPOperators
import ReactiveConcurrency

// (<|>) :: DeferredStream a -> DeferredStream a -> DeferredStream a
public func <|> <A: Sendable>(_ lhs: DeferredStream<A>, _ rhs: @autoclosure () -> DeferredStream<A>) -> DeferredStream<A> {
    DeferredStream.alt(lhs, rhs())
}
