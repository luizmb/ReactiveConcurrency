// SPDX-License-Identifier: Apache-2.0

// Monad laws for five more transformer stacks — the Reader-outer stacks (ReaderTDeferredTask,
// ReaderTDeferredStream, ReaderTPublisher) and two WriterT stacks (DeferredStreamTWriter and
// PublisherTWriter). Each suite checks left identity, right identity, associativity, and
// Kleisli/bind consistency through the stack structs and the >=> / <=< operators.

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
    private typealias T = ReactiveConcurrencyTransformers.ReaderTDeferredTask<Int, Int>
    private let f: @Sendable (Int) -> T = { n in T(Reader { env in DeferredTask { n + env + 1 } }) }
    private let g: @Sendable (Int) -> T = { n in T(Reader { _ in DeferredTask { n * 3 } }) }

    private func run(_ t: T, _ env: Int = 0) async -> Int { await t.rawValue.runReader(env).run() }

    @Test func leftIdentity() async {
        #expect(await run(T.pure(5).flatMap(f)) == run(f(5)))
    }

    @Test func rightIdentity() async {
        let m = f(9)
        #expect(await run(m.flatMap(T.pure), 2) == run(m, 2))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        #expect(await run(m.flatMap(f).flatMap(g), 1) == run(m.flatMap { a in f(a).flatMap(g) }, 1))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = await run(f(4) >>- g, 1)
        #expect(await run((f >=> g)(4), 1) == viaBind)
        #expect(await run((g <=< f)(4), 1) == viaBind)
    }

    @Test func readsTheEnvironment() async {
        #expect(await run(f(1), 10) == 12)
        #expect(await run(Reader<Int, DeferredTask<Int>>.pure(DeferredTask { 7 }).readerT) == 7)
    }
}

// MARK: - ReaderT over DeferredStream

@Suite(.timeLimit(.minutes(1))) struct ReaderTDeferredStreamLawTests {
    private typealias T = ReactiveConcurrencyTransformers.ReaderTDeferredStream<Int, Int>
    private let f: @Sendable (Int) -> T = { n in T(Reader { env in streamOf([n + env + 1, n + env + 1]) }) }
    private let g: @Sendable (Int) -> T = { n in T(Reader { _ in streamOf([n * 3, n * 3]) }) }

    private func run(_ t: T, _ env: Int = 0) async -> [Int] { await collectS(t.rawValue.runReader(env)) }

    @Test func leftIdentity() async {
        #expect(await run(T.pure(5).flatMap(f)) == run(f(5)))
    }

    @Test func rightIdentity() async {
        let m = f(9)
        #expect(await run(m.flatMap(T.pure), 2) == run(m, 2))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        #expect(await run(m.flatMap(f).flatMap(g), 1) == run(m.flatMap { a in f(a).flatMap(g) }, 1))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = await run(f(4) >>- g, 1)
        #expect(await run((f >=> g)(4), 1) == viaBind)
        #expect(await run((g <=< f)(4), 1) == viaBind)
    }

    @Test func readsTheEnvironment() async {
        #expect(await run(f(1), 10) == [12, 12])
        #expect(await run(Reader<Int, DeferredStream<Int>>.pure(streamOf([7, 7])).readerT) == [7, 7])
    }
}

// MARK: - ReaderT over Publisher

@Suite(.timeLimit(.minutes(1))) struct ReaderTPublisherLawTests {
    private typealias T = ReactiveConcurrencyTransformers.ReaderTPublisher<Int, Never, Int>
    private let f: @Sendable (Int) -> T = { n in T(Reader { env in Publisher<Int, Never>.just(n + env + 1) }) }
    private let g: @Sendable (Int) -> T = { n in T(Reader { _ in Publisher<Int, Never>.just(n * 3) }) }

    private func run(_ t: T, _ env: Int = 0) async -> [Int] { await collectP(t.rawValue.runReader(env)) }

    @Test func leftIdentity() async {
        #expect(await run(T.pure(5).flatMap(f)) == run(f(5)))
    }

    @Test func rightIdentity() async {
        let m = f(9)
        #expect(await run(m.flatMap(T.pure), 2) == run(m, 2))
    }

    @Test func associativity() async {
        let m = T.pure(2)
        #expect(await run(m.flatMap(f).flatMap(g), 1) == run(m.flatMap { a in f(a).flatMap(g) }, 1))
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = await run(f(4) >>- g, 1)
        #expect(await run((f >=> g)(4), 1) == viaBind)
        #expect(await run((g <=< f)(4), 1) == viaBind)
    }

    @Test func readsTheEnvironment() async {
        #expect(await run(f(1), 10) == [12])
        #expect(await run(Reader<Int, Publisher<Int, Never>>.pure(.just(7)).readerT) == [7])
    }
}

// MARK: - DeferredStreamTWriter

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTWriterLawTests {
    private typealias T = DeferredStreamTWriter<[String], Int>
    private let f: @Sendable (Int) -> T = { n in T(streamOf([Writer(n + 1, ["f"])])) }
    private let g: @Sendable (Int) -> T = { n in T(streamOf([Writer(n * 3, ["g"])])) }

    // Empty pure log means pure preserves the continuation's log ([] <> ["f"] == ["f"]).
    @Test func leftIdentity() async {
        #expect(await collectS(T.pure(5).flatMap(f).rawValue) == collectS(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(streamOf([Writer<[String], Int>(9, ["m"])]))
        #expect(await collectS(m.flatMap(T.pure).rawValue) == [Writer(9, ["m"])]) // ["m"] <> [] == ["m"]
    }

    @Test func associativityCombinesAllLogs() async {
        let m = T(streamOf([Writer<[String], Int>(1, ["m"])]))
        let lhs = await collectS(m.flatMap(f).flatMap(g).rawValue)
        #expect(await lhs == collectS(m.flatMap { a in f(a).flatMap(g) }.rawValue))
        #expect(lhs == [Writer(6, ["m", "f", "g"])]) // (1+1)*3 == 6; logs accumulate in order
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = await collectS((f(4) >>- g).rawValue)
        #expect(await collectS((f >=> g)(4).rawValue) == viaBind)
        #expect(await collectS((g <=< f)(4).rawValue) == viaBind)
    }
}

// MARK: - PublisherTWriter

@Suite(.timeLimit(.minutes(1))) struct PublisherTWriterLawTests {
    private typealias T = ReactiveConcurrencyTransformers.PublisherTWriter<Never, [String], Int>
    private let f: @Sendable (Int) -> T = { n in T(Publisher<Writer<[String], Int>, Never>.just(Writer(n + 1, ["f"]))) }
    private let g: @Sendable (Int) -> T = { n in T(Publisher<Writer<[String], Int>, Never>.just(Writer(n * 3, ["g"]))) }

    // Empty pure log means pure preserves the continuation's log ([] <> ["f"] == ["f"]).
    @Test func leftIdentity() async {
        #expect(await collectP(T.pure(5).flatMap(f).rawValue) == collectP(f(5).rawValue))
    }

    @Test func rightIdentity() async {
        let m = T(Publisher<Writer<[String], Int>, Never>.just(Writer<[String], Int>(9, ["m"])))
        #expect(await collectP(m.flatMap(T.pure).rawValue) == [Writer(9, ["m"])]) // ["m"] <> [] == ["m"]
    }

    @Test func associativityCombinesAllLogs() async {
        let m = T(Publisher<Writer<[String], Int>, Never>.just(Writer<[String], Int>(1, ["m"])))
        let lhs = await collectP(m.flatMap(f).flatMap(g).rawValue)
        #expect(await lhs == collectP(m.flatMap { a in f(a).flatMap(g) }.rawValue))
        #expect(lhs == [Writer(6, ["m", "f", "g"])]) // (1+1)*3 == 6; logs accumulate in order
    }

    @Test func kleisliMatchesBind() async {
        let viaBind = await collectP((f(4) >>- g).rawValue)
        #expect(await collectP((f >=> g)(4).rawValue) == viaBind)
        #expect(await collectP((g <=< f)(4).rawValue) == viaBind)
    }
}
