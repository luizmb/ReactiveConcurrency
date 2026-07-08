// SPDX-License-Identifier: Apache-2.0

// Monad laws for the transformer stacks (proves the FP 2.0 alignment is lawful, and exercises the
// newly-symmetric >=> / <=< Kleisli operators). Each suite checks left identity, right identity,
// associativity, and Kleisli/bind consistency for one stack, comparing collected values.

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
    private func pureT(_ a: Int) -> DeferredTask<Either<LawL, Int>> { DeferredTask { .right(a) } }
    private let f: @Sendable (Int) -> DeferredTask<Either<LawL, Int>> = { n in DeferredTask { .right(n + 1) } }
    private let g: @Sendable (Int) -> DeferredTask<Either<LawL, Int>> = { n in DeferredTask { .right(n * 3) } }

    @Test func leftIdentity() async {
        #expect(await runT(flatMapTDeferredTaskEither(pureT(5), f)) == runT(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await runT(flatMapTDeferredTaskEither(m) { n in DeferredTask { .right(n) } }) == runT(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredTaskEither(flatMapTDeferredTaskEither(m, f), g)
        let rhs = flatMapTDeferredTaskEither(m) { a in flatMapTDeferredTaskEither(f(a), g) }
        #expect(await runT(lhs) == runT(rhs))
    }

    // (f >=> g)(a) == f(a) >>- g, and <=< is its flip.
    @Test func kleisliMatchesBind() async {
        let viaKleisli = (f >=> g)(4)
        let viaBind = flatMapTDeferredTaskEither(f(4), g)
        #expect(await runT(viaKleisli) == runT(viaBind))
        #expect(await runT((g <=< f)(4)) == runT(viaBind))
    }

    // Short-circuit: a .left never runs the continuation.
    @Test func leftShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = DeferredTask<Either<LawL, Int>> { .left(.e) }
        let result = await runT(flatMapTDeferredTaskEither(m) { n in DeferredTask { ran = true; return .right(n) } })
        #expect(result == .left(.e))
        #expect(!ran)
    }
}

// MARK: - DeferredTaskTResult

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTResultLawTests {
    private func pureT(_ a: Int) -> DeferredTask<Result<Int, LawE>> { DeferredTask { .success(a) } }
    private let f: @Sendable (Int) -> DeferredTask<Result<Int, LawE>> = { n in DeferredTask { .success(n + 1) } }
    private let g: @Sendable (Int) -> DeferredTask<Result<Int, LawE>> = { n in DeferredTask { .success(n * 3) } }

    @Test func leftIdentity() async {
        #expect(await runT(flatMapTDeferredTaskResult(pureT(5), f)) == runT(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await runT(flatMapTDeferredTaskResult(m) { n in DeferredTask { .success(n) } }) == runT(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredTaskResult(flatMapTDeferredTaskResult(m, f), g)
        let rhs = flatMapTDeferredTaskResult(m) { a in flatMapTDeferredTaskResult(f(a), g) }
        #expect(await runT(lhs) == runT(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredTaskResult(f(4), g)
        #expect(await runT((f >=> g)(4)) == runT(viaBind))
        #expect(await runT((g <=< f)(4)) == runT(viaBind))
    }

    @Test func failureShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = DeferredTask<Result<Int, LawE>> { .failure(.boom) }
        let result = await runT(flatMapTDeferredTaskResult(m) { n in DeferredTask { ran = true; return .success(n) } })
        #expect(result == .failure(.boom))
        #expect(!ran)
    }
}

// MARK: - DeferredTaskTOptional

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTOptionalLawTests {
    private func pureT(_ a: Int) -> DeferredTask<Int?> { DeferredTask { a } }
    private let f: @Sendable (Int) -> DeferredTask<Int?> = { n in DeferredTask { n + 1 } }
    private let g: @Sendable (Int) -> DeferredTask<Int?> = { n in DeferredTask { n * 3 } }

    @Test func leftIdentity() async {
        #expect(await runT(flatMapTDeferredTaskOptional(pureT(5), f)) == runT(f(5)))
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        #expect(await runT(flatMapTDeferredTaskOptional(m) { n in DeferredTask { n } }) == runT(m))
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = flatMapTDeferredTaskOptional(flatMapTDeferredTaskOptional(m, f), g)
        let rhs = flatMapTDeferredTaskOptional(m) { a in flatMapTDeferredTaskOptional(f(a), g) }
        #expect(await runT(lhs) == runT(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredTaskOptional(f(4), g)
        #expect(await runT((f >=> g)(4)) == runT(viaBind))
        #expect(await runT((g <=< f)(4)) == runT(viaBind))
    }

    @Test func nilShortCircuits() async {
        nonisolated(unsafe) var ran = false
        let m = DeferredTask<Int?> { nil }
        let result = await runT(flatMapTDeferredTaskOptional(m) { n in DeferredTask { ran = true; return n } })
        #expect(result == nil)
        #expect(!ran)
    }
}

// MARK: - DeferredTaskTArray

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTArrayLawTests {
    private func pureT(_ a: Int) -> DeferredTask<[Int]> { DeferredTask { [a] } }
    private let f: @Sendable (Int) -> DeferredTask<[Int]> = { n in DeferredTask { [n, n + 1] } }
    private let g: @Sendable (Int) -> DeferredTask<[Int]> = { n in DeferredTask { [n * 10] } }

    @Test func leftIdentity() async {
        #expect(await runT(flatMapTDeferredTaskArray(pureT(5), f)) == runT(f(5)))
    }

    @Test func rightIdentity() async {
        let m = DeferredTask { [1, 2, 3] }
        #expect(await runT(flatMapTDeferredTaskArray(m) { n in DeferredTask { [n] } }) == runT(m))
    }

    @Test func associativity() async {
        let m = DeferredTask { [1, 2] }
        let lhs = flatMapTDeferredTaskArray(flatMapTDeferredTaskArray(m, f), g)
        let rhs = flatMapTDeferredTaskArray(m) { a in flatMapTDeferredTaskArray(f(a), g) }
        #expect(await runT(lhs) == runT(rhs))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = flatMapTDeferredTaskArray(f(4), g)
        #expect(await runT((f >=> g)(4)) == runT(viaBind))
        #expect(await runT((g <=< f)(4)) == runT(viaBind))
    }
}
