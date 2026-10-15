// SPDX-License-Identifier: Apache-2.0

// Laws for the Publisher transformer stacks. The Publisher's OWN failure channel is Never; the
// domain error lives in the inner Either/Result. Monad laws plus `<*>` == `ap` for the monad stacks,
// applicative laws for the applicative-only PublisherTArray. The stack names are qualified: on Apple,
// FP declares Combine stacks with the same names.

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

private func publisherOf<A: Sendable>(_ xs: [A]) -> Publisher<A, Never> {
    .sequence(xs)
}

// MARK: - PublisherTEither

@Suite(.timeLimit(.minutes(1))) struct PublisherTEitherLawTests {
    private typealias T = ReactiveConcurrencyTransformers.PublisherTEither<Never, LawL, Int>
    private let f: @Sendable (Int) -> T = { n in T(publisherOf([.right(n + 1)])) }
    private let g: @Sendable (Int) -> T = { n in T(publisherOf([.right(n * 3), .left(.e)])) }

    @Test func leftIdentity() async {
        #expect(await collectP(T.pure(5).flatMap(f).rawValue) == collectP(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(publisherOf([.right(1), .left(.e), .right(2)]))
        #expect(await collectP(m.flatMap(T.pure).rawValue) == collectP(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(publisherOf([.right(1), .left(.e), .right(2)]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectP(lhs.rawValue) == collectP(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectP((f >=> g)(4).rawValue) == collectP(viaBind.rawValue))
        #expect(await collectP((g <=< f)(4).rawValue) == collectP(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(publisherOf([.right(1), .left(.e), .right(2)])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(publisherOf([.right(1), .left(.e), .right(2)]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectP((fns <*> values).rawValue) == collectP(viaBind.rawValue))
    }
}

// MARK: - PublisherTResult

@Suite(.timeLimit(.minutes(1))) struct PublisherTResultLawTests {
    private typealias T = ReactiveConcurrencyTransformers.PublisherTResult<Never, LawE, Int>
    private let f: @Sendable (Int) -> T = { n in T(publisherOf([.success(n + 1)])) }
    private let g: @Sendable (Int) -> T = { n in T(publisherOf([.success(n * 3), .failure(.boom)])) }

    @Test func leftIdentity() async {
        #expect(await collectP(T.pure(5).flatMap(f).rawValue) == collectP(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(publisherOf([.success(1), .failure(.boom), .success(2)]))
        #expect(await collectP(m.flatMap(T.pure).rawValue) == collectP(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(publisherOf([.success(1), .failure(.boom), .success(2)]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectP(lhs.rawValue) == collectP(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectP((f >=> g)(4).rawValue) == collectP(viaBind.rawValue))
        #expect(await collectP((g <=< f)(4).rawValue) == collectP(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(publisherOf([.success(1), .failure(.boom), .success(2)])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(publisherOf([.success(1), .failure(.boom), .success(2)]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectP((fns <*> values).rawValue) == collectP(viaBind.rawValue))
    }
}

// MARK: - PublisherTOptional

@Suite(.timeLimit(.minutes(1))) struct PublisherTOptionalLawTests {
    private typealias T = ReactiveConcurrencyTransformers.PublisherTOptional<Never, Int>
    private let f: @Sendable (Int) -> T = { n in T(publisherOf([n + 1])) }
    private let g: @Sendable (Int) -> T = { n in T(publisherOf([n * 3, nil])) }

    @Test func leftIdentity() async {
        #expect(await collectP(T.pure(5).flatMap(f).rawValue) == collectP(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(publisherOf([1, nil, 2]))
        #expect(await collectP(m.flatMap(T.pure).rawValue) == collectP(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(publisherOf([1, nil, 2]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectP(lhs.rawValue) == collectP(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectP((f >=> g)(4).rawValue) == collectP(viaBind.rawValue))
        #expect(await collectP((g <=< f)(4).rawValue) == collectP(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(publisherOf([1, nil, 2])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(publisherOf([1, nil, 2]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectP((fns <*> values).rawValue) == collectP(viaBind.rawValue))
    }
}

// MARK: - PublisherTArray (applicative only)

@Suite(.timeLimit(.minutes(1))) struct PublisherTArrayLawTests {
    private typealias T = ReactiveConcurrencyTransformers.PublisherTArray<Never, Int>

    @Test func identity() async {
        let v = T(publisherOf([[1, 2], [3]]))
        let id = ReactiveConcurrencyTransformers.PublisherTArray<Never, @Sendable (Int) -> Int>.pure { $0 }
        #expect(await collectP((id <*> v).rawValue) == [[1, 2], [3]])
    }

    @Test func liftA2IsCartesianTwice() async {
        let lhs = T(publisherOf([[1, 2], [3]]))
        let rhs = T(publisherOf([[10], [20, 30]]))
        let sum = T.liftA2 { (a: Int, b: Int) in a + b }(lhs, rhs)
        #expect(await collectP(sum.rawValue) == [[11, 12], [21, 31, 22, 32], [13], [23, 33]])
    }
}
