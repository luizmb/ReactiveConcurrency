// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

// PublisherTEither — Publisher<Either<L, A>, F>. Inner Either.left short-circuits;
// the outer Publisher failure channel flows through unchanged.

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

@Suite struct PublisherTEitherTests {
    @Test func mapTOverRight() async {
        let p = Publisher<Either<String, Int>, Never>.just(.right(3))
        #expect(await tags(mapTPublisherEither({ $0 * 2 }, p)) == ["R6"])
    }

    @Test func mapTLeavesLeft() async {
        let p = Publisher<Either<String, Int>, Never>.just(.left("e"))
        #expect(await tags(mapTPublisherEither({ $0 * 2 }, p)) == ["Le"])
        #expect(await tags({ $0 * 2 } <£^> p) == ["Le"])
        #expect(await tags(p <&^> { $0 * 2 }) == ["Le"])
    }

    @Test func applyTCombinesRights() async {
        let fns = Publisher<Either<String, @Sendable (Int) -> Int>, Never>.just(.right { $0 + 1 })
        let vals = Publisher<Either<String, Int>, Never>.just(.right(10))
        #expect(await tags(applyTPublisherEither(fns, vals)) == ["R11"])
        #expect(await tags(fns <*> vals) == ["R11"])
    }

    @Test func liftA2TShortCircuitsOnLeft() async {
        let pa = Publisher<Either<String, Int>, Never>.just(.right(2))
        let pb = Publisher<Either<String, Int>, Never>.just(.left("boom"))
        #expect(await tags(liftA2TPublisherEither(+)(pa, pb)) == ["Lboom"])
    }

    @Test func flatMapTChainsRight() async {
        let p = Publisher<Either<String, Int>, Never>.just(.right(5))
        let chained = p >>- { n in Publisher<Either<String, Int>, Never>.just(.right(n * 2)) }
        #expect(await tags(chained) == ["R10"])
    }

    @Test func flatMapTShortCircuitsLeft() async {
        let p = Publisher<Either<String, Int>, Never>.just(.left("stop"))
        let chained = flatMapTPublisherEither(p) { n in
            Publisher<Either<String, Int>, Never>.just(.right(n * 2))
        }
        #expect(await tags(chained) == ["Lstop"])
    }

    @Test func flatMapTPreservesOrderOverSequence() async {
        let p = Publisher<Either<String, Int>, Never>.sequence([.right(1), .left("x"), .right(2)])
        let chained = p >>- { n in Publisher<Either<String, Int>, Never>.just(.right(n + 10)) }
        #expect(await tags(chained) == ["R11", "Lx", "R12"])
    }
}
