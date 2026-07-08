// SPDX-License-Identifier: Apache-2.0

// Monad laws for the Publisher transformer stacks. The Publisher's OWN failure channel is Never;
// the domain error lives in the inner Either/Result. Each suite checks left identity, right
// identity, associativity, and Kleisli/bind consistency for one stack, comparing collected values.

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private enum LawL: Error, Equatable, Sendable { case e }
private enum LawE: Error, Equatable, Sendable { case boom }

private func collectP<A: Sendable>(_ p: Publisher<A, Never>) async -> [A] {
    var out: [A] = []
    for await r in p._stream {
        if case let .success(v) = r { out.append(v) }
    }
    return out
}

// MARK: - PublisherTEither

@Suite(.timeLimit(.minutes(1))) struct PublisherTEitherLawTests {
    private func pureT(_ a: Int) -> Publisher<Either<LawL, Int>, Never> { .just(.right(a)) }
    private let f: @Sendable (Int) -> Publisher<Either<LawL, Int>, Never> = { n in .just(.right(n + 1)) }
    private let g: @Sendable (Int) -> Publisher<Either<LawL, Int>, Never> = { n in .just(.right(n * 3)) }

    @Test func leftIdentity() async {
        #expect(await collectP(flatMapTPublisherEither(pureT(5), f)) == collectP(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectP(flatMapTPublisherEither(m) { n in .just(.right(n)) }) == collectP(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTPublisherEither(flatMapTPublisherEither(m, f), g)
        let rhs = flatMapTPublisherEither(m) { a in flatMapTPublisherEither(f(a), g) }
        #expect(await collectP(lhs) == collectP(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTPublisherEither(f(4), g)
        #expect(await collectP((f >=> g)(4)) == collectP(viaBind))
        #expect(await collectP((g <=< f)(4)) == collectP(viaBind))
    }
}

// MARK: - PublisherTResult

@Suite(.timeLimit(.minutes(1))) struct PublisherTResultLawTests {
    private func pureT(_ a: Int) -> Publisher<Result<Int, LawE>, Never> { .just(.success(a)) }
    private let f: @Sendable (Int) -> Publisher<Result<Int, LawE>, Never> = { n in .just(.success(n + 1)) }
    private let g: @Sendable (Int) -> Publisher<Result<Int, LawE>, Never> = { n in .just(.success(n * 3)) }

    @Test func leftIdentity() async {
        #expect(await collectP(flatMapTPublisherResult(pureT(5), f)) == collectP(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectP(flatMapTPublisherResult(m) { n in .just(.success(n)) }) == collectP(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTPublisherResult(flatMapTPublisherResult(m, f), g)
        let rhs = flatMapTPublisherResult(m) { a in flatMapTPublisherResult(f(a), g) }
        #expect(await collectP(lhs) == collectP(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTPublisherResult(f(4), g)
        #expect(await collectP((f >=> g)(4)) == collectP(viaBind))
        #expect(await collectP((g <=< f)(4)) == collectP(viaBind))
    }
}

// MARK: - PublisherTOptional

@Suite(.timeLimit(.minutes(1))) struct PublisherTOptionalLawTests {
    private func pureT(_ a: Int) -> Publisher<Int?, Never> { .just(a as Int?) }
    private let f: @Sendable (Int) -> Publisher<Int?, Never> = { n in .just(n + 1 as Int?) }
    private let g: @Sendable (Int) -> Publisher<Int?, Never> = { n in .just(n * 3 as Int?) }

    @Test func leftIdentity() async {
        #expect(await collectP(flatMapTPublisherOptional(pureT(5), f)) == collectP(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await collectP(flatMapTPublisherOptional(m) { n in .just(n as Int?) }) == collectP(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTPublisherOptional(flatMapTPublisherOptional(m, f), g)
        let rhs = flatMapTPublisherOptional(m) { a in flatMapTPublisherOptional(f(a), g) }
        #expect(await collectP(lhs) == collectP(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTPublisherOptional(f(4), g)
        #expect(await collectP((f >=> g)(4)) == collectP(viaBind))
        #expect(await collectP((g <=< f)(4)) == collectP(viaBind))
    }
}

// MARK: - PublisherTArray

@Suite(.timeLimit(.minutes(1))) struct PublisherTArrayLawTests {
    private func pureT(_ a: Int) -> Publisher<[Int], Never> { .just([a]) }
    private let f: @Sendable (Int) -> Publisher<[Int], Never> = { n in .just([n, n + 1]) }
    private let g: @Sendable (Int) -> Publisher<[Int], Never> = { n in .just([n * 10]) }

    @Test func leftIdentity() async {
        #expect(await collectP(flatMapTPublisherArray(pureT(5), f)) == collectP(f(5)))
    }

    @Test func rightIdentity() async {
        let m = Publisher<[Int], Never>.just([1, 2, 3])
        #expect(await collectP(flatMapTPublisherArray(m) { n in .just([n]) }) == collectP(m))
    }

    @Test func associativity() async {
        let m = Publisher<[Int], Never>.just([1, 2])
        let lhs = flatMapTPublisherArray(flatMapTPublisherArray(m, f), g)
        let rhs = flatMapTPublisherArray(m) { a in flatMapTPublisherArray(f(a), g) }
        #expect(await collectP(lhs) == collectP(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTPublisherArray(f(4), g)
        #expect(await collectP((f >=> g)(4)) == collectP(viaBind))
        #expect(await collectP((g <=< f)(4)) == collectP(viaBind))
    }
}
