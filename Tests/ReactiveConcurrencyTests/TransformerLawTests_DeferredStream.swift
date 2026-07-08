// SPDX-License-Identifier: Apache-2.0

// Monad laws for the DeferredStream transformer stacks. Each suite checks left identity, right
// identity, associativity, and Kleisli/bind consistency for one stack, comparing collected values.

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private enum LawL: Error, Equatable, Sendable { case e }
private enum LawE: Error, Equatable, Sendable { case boom }

private func streamOf<A: Sendable>(_ xs: [A]) -> DeferredStream<A> {
    DeferredStream<A> {
        AsyncStream { c in
            for x in xs {
                c.yield(x)
            }
            c.finish()
        }
    }
}

private func collectS<A: Sendable>(_ s: DeferredStream<A>) async -> [A] {
    var out: [A] = []
    for await x in s {
        out.append(x)
    }
    return out
}

// MARK: - DeferredStreamTEither

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTEitherLawTests {
    private func pureT(_ a: Int) -> DeferredStream<Either<LawL, Int>> { streamOf([.right(a)]) }
    private let f: @Sendable (Int) -> DeferredStream<Either<LawL, Int>> = { n in streamOf([.right(n + 1)]) }
    private let g: @Sendable (Int) -> DeferredStream<Either<LawL, Int>> = { n in streamOf([.right(n * 3)]) }

    @Test func leftIdentity() async {
        #expect(await collectS(flatMapTDeferredStreamEither(pureT(5), f)) == collectS(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectS(flatMapTDeferredStreamEither(m) { n in streamOf([.right(n)]) }) == collectS(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredStreamEither(flatMapTDeferredStreamEither(m, f), g)
        let rhs = flatMapTDeferredStreamEither(m) { a in flatMapTDeferredStreamEither(f(a), g) }
        #expect(await collectS(lhs) == collectS(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredStreamEither(f(4), g)
        #expect(await collectS((f >=> g)(4)) == collectS(viaBind))
        #expect(await collectS((g <=< f)(4)) == collectS(viaBind))
    }
}

// MARK: - DeferredStreamTResult

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTResultLawTests {
    private func pureT(_ a: Int) -> DeferredStream<Result<Int, LawE>> { streamOf([.success(a)]) }
    private let f: @Sendable (Int) -> DeferredStream<Result<Int, LawE>> = { n in streamOf([.success(n + 1)]) }
    private let g: @Sendable (Int) -> DeferredStream<Result<Int, LawE>> = { n in streamOf([.success(n * 3)]) }

    @Test func leftIdentity() async {
        #expect(await collectS(flatMapTDeferredStreamResult(pureT(5), f)) == collectS(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectS(flatMapTDeferredStreamResult(m) { n in streamOf([.success(n)]) }) == collectS(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredStreamResult(flatMapTDeferredStreamResult(m, f), g)
        let rhs = flatMapTDeferredStreamResult(m) { a in flatMapTDeferredStreamResult(f(a), g) }
        #expect(await collectS(lhs) == collectS(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredStreamResult(f(4), g)
        #expect(await collectS((f >=> g)(4)) == collectS(viaBind))
        #expect(await collectS((g <=< f)(4)) == collectS(viaBind))
    }
}

// MARK: - DeferredStreamTOptional

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTOptionalLawTests {
    private func pureT(_ a: Int) -> DeferredStream<Int?> { streamOf([Optional(a)]) }
    private let f: @Sendable (Int) -> DeferredStream<Int?> = { n in streamOf([Optional(n + 1)]) }
    private let g: @Sendable (Int) -> DeferredStream<Int?> = { n in streamOf([Optional(n * 3)]) }

    @Test func leftIdentity() async {
        #expect(await collectS(flatMapTDeferredStreamOptional(pureT(5), f)) == collectS(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectS(flatMapTDeferredStreamOptional(m) { n in streamOf([Optional(n)]) }) == collectS(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredStreamOptional(flatMapTDeferredStreamOptional(m, f), g)
        let rhs = flatMapTDeferredStreamOptional(m) { a in flatMapTDeferredStreamOptional(f(a), g) }
        #expect(await collectS(lhs) == collectS(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredStreamOptional(f(4), g)
        #expect(await collectS((f >=> g)(4)) == collectS(viaBind))
        #expect(await collectS((g <=< f)(4)) == collectS(viaBind))
    }
}

// MARK: - DeferredStreamTArray

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTArrayLawTests {
    private func pureT(_ a: Int) -> DeferredStream<[Int]> { streamOf([[a]]) }
    private let f: @Sendable (Int) -> DeferredStream<[Int]> = { n in streamOf([[n, n + 1]]) }
    private let g: @Sendable (Int) -> DeferredStream<[Int]> = { n in streamOf([[n * 3]]) }

    @Test func leftIdentity() async {
        #expect(await collectS(flatMapTDeferredStreamArray(pureT(5), f)) == collectS(f(5)))
    }

    @Test func rightIdentity() async {
        let m = streamOf([[1, 2, 3]])
        #expect(await collectS(flatMapTDeferredStreamArray(m) { n in streamOf([[n]]) }) == collectS(m))
    }

    @Test func associativity() async {
        let m = streamOf([[1, 2]])
        let lhs = flatMapTDeferredStreamArray(flatMapTDeferredStreamArray(m, f), g)
        let rhs = flatMapTDeferredStreamArray(m) { a in flatMapTDeferredStreamArray(f(a), g) }
        #expect(await collectS(lhs) == collectS(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredStreamArray(f(4), g)
        #expect(await collectS((f >=> g)(4)) == collectS(viaBind))
        #expect(await collectS((g <=< f)(4)) == collectS(viaBind))
    }
}
