// SPDX-License-Identifier: Apache-2.0

// Laws for the DeferredTask transformer stacks. The monad stacks check left identity, right
// identity, associativity, Kleisli/bind consistency, short-circuiting, and that `<*>` agrees with
// `>>-` (`ap`). The applicative-only stack (DeferredTaskTArray, `ListT` has no lawful monad) checks the
// applicative laws.

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private enum LawL: Error, Equatable, Sendable { case e }
private enum LawE: Error, Equatable, Sendable { case boom }

private func runT<A: Sendable>(_ task: DeferredTask<A>) async -> A { await task.run() }

// MARK: - DeferredTaskTEither

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTEitherLawTests {
    private typealias T = DeferredTaskTEither<LawL, Int>
    private let f: @Sendable (Int) -> T = { n in T(DeferredTask { .right(n + 1) }) }
    private let g: @Sendable (Int) -> T = { n in T(DeferredTask { .right(n * 3) }) }

    @Test func pureIsRight() async {
        #expect(await runT(T.pure(5).rawValue) == .right(5))
    }

    @Test func leftIdentity() async {
        #expect(await runT(T.pure(5).flatMap(f).rawValue) == runT(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T.pure(9)
        #expect(await runT(m.flatMap(T.pure).rawValue) == runT(m.rawValue))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await runT(lhs.rawValue) == runT(rhs.rawValue))
    }

    // (f >=> g)(a) == f(a) >>- g, and <=< is its flip.
    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await runT((f >=> g)(4).rawValue) == runT(viaBind.rawValue))
        #expect(await runT((g <=< f)(4).rawValue) == runT(viaBind.rawValue))
    }

    // Short-circuit: a .left never runs the continuation.
    @Test func leftShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = T(DeferredTask { .left(.e) })
        let result = await runT(m.flatMap { n in T(DeferredTask { ran = true; return .right(n) }) }.rawValue)
        #expect(result == .left(.e))
        #expect(!ran)
    }

    // `<*>` is `ap`: a .left on the left never runs the right effect.
    @Test func applyAgreesWithBind() async {
        nonisolated(unsafe) var ran = false
        let fns = T(DeferredTask { .left(.e) }).map { (_: Int) -> @Sendable (Int) -> Int in { $0 } }
        let values = T(DeferredTask { ran = true; return .right(1) })
        #expect(await runT((fns <*> values).rawValue) == .left(.e))
        #expect(!ran)
        let plus = T.pure(10).map { a in { @Sendable b in a + b } }
        #expect(await runT((plus <*> T.pure(5)).rawValue) == .right(15))
    }
}

// MARK: - DeferredTaskTResult

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTResultLawTests {
    private typealias T = DeferredTaskTResult<LawE, Int>
    private let f: @Sendable (Int) -> T = { n in T(DeferredTask { .success(n + 1) }) }
    private let g: @Sendable (Int) -> T = { n in T(DeferredTask { .success(n * 3) }) }

    @Test func leftIdentity() async {
        #expect(await runT(T.pure(5).flatMap(f).rawValue) == runT(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T.pure(9)
        #expect(await runT(m.flatMap(T.pure).rawValue) == runT(m.rawValue))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await runT(lhs.rawValue) == runT(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await runT((f >=> g)(4).rawValue) == runT(viaBind.rawValue))
        #expect(await runT((g <=< f)(4).rawValue) == runT(viaBind.rawValue))
    }

    @Test func failureShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = T(DeferredTask { .failure(.boom) })
        let result = await runT(m.flatMap { n in T(DeferredTask { ran = true; return .success(n) }) }.rawValue)
        #expect(result == .failure(.boom))
        #expect(!ran)
    }

    @Test func liftA2ShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let failing = T(DeferredTask { .failure(.boom) })
        let other = T(DeferredTask { ran = true; return .success(1) })
        #expect(await runT(T.liftA2 { (a: Int, b: Int) in a + b }(failing, other).rawValue) == .failure(.boom))
        #expect(!ran)
        #expect(await runT((T.pure(1) *> T.pure(2)).rawValue) == .success(2))
        #expect(await runT((T.pure(1) <* T.pure(2)).rawValue) == .success(1))
    }
}

// MARK: - DeferredTaskTOptional

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTOptionalLawTests {
    private typealias T = DeferredTaskTOptional<Int>
    private let f: @Sendable (Int) -> T = { n in T(DeferredTask { n + 1 }) }
    private let g: @Sendable (Int) -> T = { n in T(DeferredTask { n * 3 }) }

    @Test func leftIdentity() async {
        #expect(await runT(T.pure(5).flatMap(f).rawValue) == runT(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T.pure(9)
        #expect(await runT(m.flatMap(T.pure).rawValue) == runT(m.rawValue))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await runT(lhs.rawValue) == runT(rhs.rawValue))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = f(4) >>- g
        #expect(await runT((f >=> g)(4).rawValue) == runT(viaBind.rawValue))
        #expect(await runT((g <=< f)(4).rawValue) == runT(viaBind.rawValue))
    }

    @Test func nilShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = T(DeferredTask { nil })
        let result = await runT(m.flatMap { n in T(DeferredTask { ran = true; return n }) }.rawValue)
        #expect(result == nil)
        #expect(!ran)
    }

    @Test func liftingProperty() async {
        let lifted = DeferredTask<Int?> { 4 }.deferredTaskT
        #expect(await runT((lifted <&> { $0 * 2 }).rawValue) == 8)
    }
}

// MARK: - DeferredTaskTArray (applicative only)

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTArrayLawTests {
    private typealias T = DeferredTaskTArray<Int>

    @Test func identity() async {
        let v = T(DeferredTask { [1, 2, 3] })
        let id = DeferredTaskTArray<@Sendable (Int) -> Int>.pure { $0 }
        #expect(await runT((id <*> v).rawValue) == [1, 2, 3])
    }

    @Test func homomorphism() async {
        let fn: @Sendable (Int) -> Int = { $0 + 1 }
        let lhs = DeferredTaskTArray.pure(fn) <*> T.pure(4)
        #expect(await runT(lhs.rawValue) == [5])
    }

    // liftA2 over the inner arrays is the cartesian product, left-major.
    @Test func liftA2IsCartesian() async {
        let lhs = T(DeferredTask { [1, 2] })
        let rhs = T(DeferredTask { [10, 20] })
        #expect(await runT(T.liftA2 { (a: Int, b: Int) in a + b }(lhs, rhs).rawValue) == [11, 21, 12, 22])
    }

    @Test func functorMap() async {
        let v = T(DeferredTask { [1, 2] })
        #expect(await runT(({ $0 * 3 } <£> v).rawValue) == [3, 6])
        #expect(await runT((v £> 7).rawValue) == [7, 7])
    }
}
