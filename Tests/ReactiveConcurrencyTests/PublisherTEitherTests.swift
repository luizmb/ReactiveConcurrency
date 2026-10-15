// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

// PublisherTEither — Publisher<Either<L, A>, F>. Inner Either.left short-circuits;
// the outer Publisher failure channel flows through unchanged. Qualified: FP has a Combine
// `PublisherTEither` on Apple.

private typealias PEither<A: Sendable> = ReactiveConcurrencyTransformers.PublisherTEither<Never, String, A>

private func tags<L: Sendable, A: Sendable, F: Error>(
    _ publisher: Publisher<Either<L, A>, F>
) async -> [String] {
    var out: [String] = []
    for await result in publisher._stream {
        if case let .success(either) = result {
            switch either {
            case let .right(r): out.append("R\(r)")
            case let .left(l): out.append("L\(l)")
            }
        }
    }
    return out
}

@Suite(.timeLimit(.minutes(1))) struct PublisherTEitherTests {
    @Test func mapOverRight() async {
        #expect(await tags(PEither(.just(.right(3))).map { $0 * 2 }.rawValue) == ["R6"])
    }

    @Test func mapLeavesLeft() async {
        let p = PEither<Int>(.just(.left("e")))
        #expect(await tags(p.map { $0 * 2 }.rawValue) == ["Le"])
        #expect(await tags(({ $0 * 2 } <£> p).rawValue) == ["Le"])
        #expect(await tags((p <&> { $0 * 2 }).rawValue) == ["Le"])
    }

    @Test func applyCombinesRights() async {
        let fns = PEither<@Sendable (Int) -> Int>(.just(.right { $0 + 1 }))
        let vals = PEither<Int>(.just(.right(10)))
        #expect(await tags(PEither.apply(fns, vals).rawValue) == ["R11"])
        #expect(await tags((fns <*> vals).rawValue) == ["R11"])
    }

    @Test func liftA2ShortCircuitsOnLeft() async {
        let pa = PEither<Int>(.just(.right(2)))
        let pb = PEither<Int>(.just(.left("boom")))
        #expect(await tags(PEither.liftA2(+)(pa, pb).rawValue) == ["Lboom"])
    }

    @Test func flatMapChainsRight() async {
        let chained = PEither(.just(.right(5))) >>- { n in PEither(.just(.right(n * 2))) }
        #expect(await tags(chained.rawValue) == ["R10"])
    }

    @Test func flatMapShortCircuitsLeft() async {
        let chained = PEither<Int>(.just(.left("stop"))).flatMap { n in PEither(.just(.right(n * 2))) }
        #expect(await tags(chained.rawValue) == ["Lstop"])
    }

    @Test func flatMapPreservesOrderOverSequence() async {
        let p = PEither<Int>(.sequence([.right(1), .left("x"), .right(2)]))
        let chained = p >>- { n in PEither(.just(.right(n + 10))) }
        #expect(await tags(chained.rawValue) == ["R11", "Lx", "R12"])
    }

    // `<*>` is `ap` over the stream: every function against every value, in order.
    @Test func applyIsCartesianInOrder() async {
        let fns = PEither<@Sendable (Int) -> Int>(.sequence([.right { $0 + 1 }, .right { $0 * 10 }]))
        let vals = PEither<Int>(.sequence([.right(1), .right(2)]))
        #expect(await tags((fns <*> vals).rawValue) == ["R2", "R3", "R10", "R20"])
    }
}
