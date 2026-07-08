// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency

// StatefulTPublisher: outer = Stateful, inner = Publisher
// Type: Stateful<S, Publisher<A, F>>
// State threads synchronously; streaming happens later when subscribed.
// (No Monad: capturing inout state across the async stream boundary isn't expressible —
// matches StatefulTDeferredStream.)

/// Applicative apply for the Stateful-over-Publisher stack.
public func applyStatefulPublisher<S, A: Sendable, B: Sendable, F: Error>(
    _ sf: Stateful<S, Publisher<@Sendable (A) -> B, F>>,
    _ sa: Stateful<S, Publisher<A, F>>
) -> Stateful<S, Publisher<B, F>> {
    Stateful<S, Publisher<B, F>> { s in
        let pf = sf.run(&s)
        let pa = sa.run(&s)
        return applyPublisher(pf, pa)
    }
}

/// Applicative liftA2 for the Stateful-over-Publisher stack: runs both effects and combines their results with fn.
public func liftA2StatefulPublisher<S, A: Sendable, B: Sendable, C: Sendable, F: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Stateful<S, Publisher<A, F>>, Stateful<S, Publisher<B, F>>) -> Stateful<S, Publisher<C, F>> {
    { sa, sb in
        Stateful<S, Publisher<C, F>> { s in
            let pa = sa.run(&s)
            let pb = sb.run(&s)
            return pa.zip(pb).map { fn($0.0, $0.1) }
        }
    }
}

/// Applicative seqRight for the Stateful-over-Publisher stack: sequences both effects, keeps the right result.
public func seqRightStatefulPublisher<S, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Stateful<S, Publisher<A, F>>,
    _ rhs: Stateful<S, Publisher<B, F>>
) -> Stateful<S, Publisher<B, F>> {
    Stateful<S, Publisher<B, F>> { s in lhs.run(&s).seqRight(rhs.run(&s)) }
}

/// Applicative seqLeft for the Stateful-over-Publisher stack: sequences both effects, keeps the left result.
public func seqLeftStatefulPublisher<S, A: Sendable, B: Sendable, F: Error>(
    _ lhs: Stateful<S, Publisher<A, F>>,
    _ rhs: Stateful<S, Publisher<B, F>>
) -> Stateful<S, Publisher<A, F>> {
    Stateful<S, Publisher<A, F>> { s in lhs.run(&s).seqLeft(rhs.run(&s)) }
}
