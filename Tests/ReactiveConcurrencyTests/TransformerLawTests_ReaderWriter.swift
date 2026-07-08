// SPDX-License-Identifier: Apache-2.0

// Monad laws for five more transformer stacks — the Reader-outer stacks (ReaderTDeferredTask,
// ReaderTDeferredStream, ReaderTPublisher) and two additional WriterT stacks (WriterT over
// DeferredStream and over Publisher). Each suite checks left identity, right identity,
// associativity, and Kleisli/bind consistency, exercising the `.flatMapT` method and the
// symmetric >=> / <=< Kleisli operators. WriterT-over-DeferredTask is covered by WriterTLawTests.

import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

// MARK: - Collection helpers

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
    var o: [A] = []
    for await x in s {
        o.append(x)
    }
    return o
}

private func collectP<A: Sendable>(_ p: Publisher<A, Never>) async -> [A] {
    var o: [A] = []
    for await r in p._stream {
        if case let .success(v) = r { o.append(v) }
    }
    return o
}

// MARK: - ReaderT over DeferredTask

@Suite(.timeLimit(.minutes(1))) struct ReaderTDeferredTaskLawTests {
    private func pureT(_ a: Int) -> Reader<Int, DeferredTask<Int>> {
        Reader<Int, DeferredTask<Int>> { _ in DeferredTask { a } }
    }

    private let f: @Sendable (Int) -> Reader<Int, DeferredTask<Int>> = { n in
        Reader<Int, DeferredTask<Int>> { _ in DeferredTask { n + 1 } }
    }

    private let g: @Sendable (Int) -> Reader<Int, DeferredTask<Int>> = { n in
        Reader<Int, DeferredTask<Int>> { _ in DeferredTask { n * 3 } }
    }

    @Test func leftIdentity() async {
        let lhs = await pureT(5).flatMapT(f).runReader(0).run()
        let rhs = await f(5).runReader(0).run()
        #expect(lhs == rhs)
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        let lhs = await m.flatMapT { n in Reader<Int, DeferredTask<Int>> { _ in DeferredTask { n } } }.runReader(0).run()
        let rhs = await m.runReader(0).run()
        #expect(lhs == rhs)
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = await m.flatMapT(f).flatMapT(g).runReader(0).run()
        let rhs = await m.flatMapT { a in f(a).flatMapT(g) }.runReader(0).run()
        #expect(lhs == rhs)
    }

    @Test func kleisliMatchesBind() async {
        let viaKleisli = await (f >=> g)(4).runReader(0).run()
        let viaBind = await f(4).flatMapT(g).runReader(0).run()
        #expect(viaKleisli == viaBind)
        #expect(await (g <=< f)(4).runReader(0).run() == viaBind)
    }
}

// MARK: - ReaderT over DeferredStream

@Suite(.timeLimit(.minutes(1))) struct ReaderTDeferredStreamLawTests {
    private func pureT(_ a: Int) -> Reader<Int, DeferredStream<Int>> {
        Reader<Int, DeferredStream<Int>> { _ in streamOf([a]) }
    }

    private let f: @Sendable (Int) -> Reader<Int, DeferredStream<Int>> = { n in
        Reader<Int, DeferredStream<Int>> { _ in streamOf([n + 1]) }
    }

    private let g: @Sendable (Int) -> Reader<Int, DeferredStream<Int>> = { n in
        Reader<Int, DeferredStream<Int>> { _ in streamOf([n * 3]) }
    }

    @Test func leftIdentity() async {
        let lhs = await collectS(pureT(5).flatMapT(f).runReader(0))
        let rhs = await collectS(f(5).runReader(0))
        #expect(lhs == rhs)
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        let lhs = await collectS(m.flatMapT { n in Reader<Int, DeferredStream<Int>> { _ in streamOf([n]) } }.runReader(0))
        let rhs = await collectS(m.runReader(0))
        #expect(lhs == rhs)
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = await collectS(m.flatMapT(f).flatMapT(g).runReader(0))
        let rhs = await collectS(m.flatMapT { a in f(a).flatMapT(g) }.runReader(0))
        #expect(lhs == rhs)
    }

    @Test func kleisliMatchesBind() async {
        let viaKleisli = await collectS((f >=> g)(4).runReader(0))
        let viaBind = await collectS(f(4).flatMapT(g).runReader(0))
        #expect(viaKleisli == viaBind)
        #expect(await collectS((g <=< f)(4).runReader(0)) == viaBind)
    }
}

// MARK: - ReaderT over Publisher

@Suite(.timeLimit(.minutes(1))) struct ReaderTPublisherLawTests {
    private func pureT(_ a: Int) -> Reader<Int, Publisher<Int, Never>> {
        Reader<Int, Publisher<Int, Never>> { _ in .just(a) }
    }

    private let f: @Sendable (Int) -> Reader<Int, Publisher<Int, Never>> = { n in
        Reader<Int, Publisher<Int, Never>> { _ in .just(n + 1) }
    }

    private let g: @Sendable (Int) -> Reader<Int, Publisher<Int, Never>> = { n in
        Reader<Int, Publisher<Int, Never>> { _ in .just(n * 3) }
    }

    @Test func leftIdentity() async {
        let lhs = await collectP(pureT(5).flatMapT(f).runReader(0))
        let rhs = await collectP(f(5).runReader(0))
        #expect(lhs == rhs)
    }

    @Test func rightIdentity() async {
        let m = pureT(9)
        let lhs = await collectP(m.flatMapT { n in Reader<Int, Publisher<Int, Never>> { _ in .just(n) } }.runReader(0))
        let rhs = await collectP(m.runReader(0))
        #expect(lhs == rhs)
    }

    @Test func associativity() async {
        let m = pureT(2)
        let lhs = await collectP(m.flatMapT(f).flatMapT(g).runReader(0))
        let rhs = await collectP(m.flatMapT { a in f(a).flatMapT(g) }.runReader(0))
        #expect(lhs == rhs)
    }

    @Test func kleisliMatchesBind() async {
        let viaKleisli = await collectP((f >=> g)(4).runReader(0))
        let viaBind = await collectP(f(4).flatMapT(g).runReader(0))
        #expect(viaKleisli == viaBind)
        #expect(await collectP((g <=< f)(4).runReader(0)) == viaBind)
    }
}

// MARK: - WriterT over DeferredStream

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTWriterLawTests {
    private func pureT(_ a: Int) -> DeferredStream<Writer<[String], Int>> {
        streamOf([Writer(a, [])])
    }

    private let f: @Sendable (Int) -> DeferredStream<Writer<[String], Int>> = { n in
        streamOf([Writer(n + 1, ["f"])])
    }

    private let g: @Sendable (Int) -> DeferredStream<Writer<[String], Int>> = { n in
        streamOf([Writer(n * 3, ["g"])])
    }

    // Empty pure log means pure preserves the continuation's log ([] <> ["f"] == ["f"]).
    @Test func leftIdentity() async {
        let lhs = await collectS(pureT(5).flatMapT(f))
        let rhs = await collectS(f(5))
        #expect(lhs == rhs)
    }

    @Test func rightIdentity() async {
        let m = streamOf([Writer<[String], Int>(9, ["m"])])
        let result = await collectS(m.flatMapT { (n: Int) in streamOf([Writer<[String], Int>(n, [])]) })
        #expect(result == [Writer(9, ["m"])]) // ["m"] <> [] == ["m"]
    }

    @Test func associativityCombinesAllLogs() async {
        let m = streamOf([Writer<[String], Int>(1, ["m"])])
        let lhs = await collectS(m.flatMapT(f).flatMapT(g))
        let rhs = await collectS(m.flatMapT { a in f(a).flatMapT(g) })
        #expect(lhs == rhs)
        #expect(lhs == [Writer(6, ["m", "f", "g"])]) // (1+1)*3 == 6; logs accumulate in order
    }

    @Test func kleisliMatchesBind() async {
        let viaKleisli = await collectS((f >=> g)(4))
        let viaBind = await collectS(f(4).flatMapT(g))
        #expect(viaKleisli == viaBind)
        #expect(await collectS((g <=< f)(4)) == viaBind)
    }
}

// MARK: - WriterT over Publisher

@Suite(.timeLimit(.minutes(1))) struct PublisherTWriterLawTests {
    private func pureT(_ a: Int) -> Publisher<Writer<[String], Int>, Never> {
        .just(Writer(a, []))
    }

    private let f: @Sendable (Int) -> Publisher<Writer<[String], Int>, Never> = { n in
        .just(Writer(n + 1, ["f"]))
    }

    private let g: @Sendable (Int) -> Publisher<Writer<[String], Int>, Never> = { n in
        .just(Writer(n * 3, ["g"]))
    }

    @Test func leftIdentity() async {
        let lhs = await collectP(pureT(5).flatMapT(f))
        let rhs = await collectP(f(5))
        #expect(lhs == rhs)
    }

    @Test func rightIdentity() async {
        let m = Publisher<Writer<[String], Int>, Never>.just(Writer(9, ["m"]))
        let result = await collectP(m.flatMapT { (n: Int) in Publisher<Writer<[String], Int>, Never>.just(Writer(n, [])) })
        #expect(result == [Writer(9, ["m"])]) // ["m"] <> [] == ["m"]
    }

    @Test func associativityCombinesAllLogs() async {
        let m = Publisher<Writer<[String], Int>, Never>.just(Writer(1, ["m"]))
        let lhs = await collectP(m.flatMapT(f).flatMapT(g))
        let rhs = await collectP(m.flatMapT { a in f(a).flatMapT(g) })
        #expect(lhs == rhs)
        #expect(lhs == [Writer(6, ["m", "f", "g"])]) // (1+1)*3 == 6; logs accumulate in order
    }

    @Test func kleisliMatchesBind() async {
        let viaKleisli = await collectP((f >=> g)(4))
        let viaBind = await collectP(f(4).flatMapT(g))
        #expect(viaKleisli == viaBind)
        #expect(await collectP((g <=< f)(4)) == viaBind)
    }
}
