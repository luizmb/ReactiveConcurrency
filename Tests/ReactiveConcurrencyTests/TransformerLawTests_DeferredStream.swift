// SPDX-License-Identifier: Apache-2.0

// Laws for the DeferredStream transformer stacks: monad laws plus `<*>` == `ap` for the monad stacks,
// applicative laws for the applicative-only DeferredStreamTArray (`ListT` has no lawful monad).

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
    private typealias T = DeferredStreamTEither<LawL, Int>
    private let f: @Sendable (Int) -> T = { n in T(streamOf([.right(n + 1)])) }
    private let g: @Sendable (Int) -> T = { n in T(streamOf([.right(n * 3), .left(.e)])) }

    @Test func leftIdentity() async {
        #expect(await collectS(T.pure(5).flatMap(f).rawValue) == collectS(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(streamOf([.right(1), .left(.e), .right(2)]))
        #expect(await collectS(m.flatMap(T.pure).rawValue) == collectS(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(streamOf([.right(1), .left(.e), .right(2)]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectS(lhs.rawValue) == collectS(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectS((f >=> g)(4).rawValue) == collectS(viaBind.rawValue))
        #expect(await collectS((g <=< f)(4).rawValue) == collectS(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(streamOf([.right(1), .left(.e), .right(2)])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(streamOf([.right(1), .left(.e), .right(2)]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectS((fns <*> values).rawValue) == collectS(viaBind.rawValue))
    }
}

// MARK: - DeferredStreamTResult

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTResultLawTests {
    private typealias T = DeferredStreamTResult<LawE, Int>
    private let f: @Sendable (Int) -> T = { n in T(streamOf([.success(n + 1)])) }
    private let g: @Sendable (Int) -> T = { n in T(streamOf([.success(n * 3), .failure(.boom)])) }

    @Test func leftIdentity() async {
        #expect(await collectS(T.pure(5).flatMap(f).rawValue) == collectS(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(streamOf([.success(1), .failure(.boom), .success(2)]))
        #expect(await collectS(m.flatMap(T.pure).rawValue) == collectS(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(streamOf([.success(1), .failure(.boom), .success(2)]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectS(lhs.rawValue) == collectS(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectS((f >=> g)(4).rawValue) == collectS(viaBind.rawValue))
        #expect(await collectS((g <=< f)(4).rawValue) == collectS(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(streamOf([.success(1), .failure(.boom), .success(2)])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(streamOf([.success(1), .failure(.boom), .success(2)]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectS((fns <*> values).rawValue) == collectS(viaBind.rawValue))
    }
}

// MARK: - DeferredStreamTOptional

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTOptionalLawTests {
    private typealias T = DeferredStreamTOptional<Int>
    private let f: @Sendable (Int) -> T = { n in T(streamOf([n + 1])) }
    private let g: @Sendable (Int) -> T = { n in T(streamOf([n * 3, nil])) }

    @Test func leftIdentity() async {
        #expect(await collectS(T.pure(5).flatMap(f).rawValue) == collectS(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(streamOf([1, nil, 2]))
        #expect(await collectS(m.flatMap(T.pure).rawValue) == collectS(m.rawValue))
    }

    @Test func associativity() async {
        let m = T(streamOf([1, nil, 2]))
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collectS(lhs.rawValue) == collectS(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await collectS((f >=> g)(4).rawValue) == collectS(viaBind.rawValue))
        #expect(await collectS((g <=< f)(4).rawValue) == collectS(viaBind.rawValue))
    }

    // `<*>` is `ap`, derived from `>>-`: for each function, in order, every value.
    @Test func applyIsAp() async {
        let fns = T(streamOf([1, nil, 2])).map { a in { @Sendable (b: Int) in a * 10 + b } }
        let values = T(streamOf([1, nil, 2]))
        let viaBind = fns.flatMap { fn in values.map(fn) }
        #expect(await collectS((fns <*> values).rawValue) == collectS(viaBind.rawValue))
    }

    // A nil function short-circuits once; it never re-runs the value stream per element.
    @Test func nilFunctionEmitsOneNil() async {
        let fns = T(streamOf([nil, 1])).map { a in { @Sendable (b: Int) in a + b } }
        let values = T(streamOf([10, 20]))
        #expect(await collectS((fns <*> values).rawValue) == [nil, 11, 21])
    }
}

// MARK: - DeferredStreamTArray (applicative only)

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTArrayLawTests {
    private typealias T = DeferredStreamTArray<Int>

    @Test func identity() async {
        let v = T(streamOf([[1, 2], [3]]))
        let id = DeferredStreamTArray<@Sendable (Int) -> Int>.pure { $0 }
        #expect(await collectS((id <*> v).rawValue) == [[1, 2], [3]])
    }

    // Outer: cartesian over the stream elements; inner: cartesian over each pair of arrays.
    @Test func liftA2IsCartesianTwice() async {
        let lhs = T(streamOf([[1, 2], [3]]))
        let rhs = T(streamOf([[10], [20, 30]]))
        let sum = T.liftA2 { (a: Int, b: Int) in a + b }(lhs, rhs)
        #expect(await collectS(sum.rawValue) == [[11, 12], [21, 31, 22, 32], [13], [23, 33]])
    }

    @Test func liftingProperty() async {
        let lifted = streamOf([[1, 2]]).deferredStreamT
        #expect(await collectS((lifted <&> { $0 + 1 }).rawValue) == [[2, 3]])
    }
}
