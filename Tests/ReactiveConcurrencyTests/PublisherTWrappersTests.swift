// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

// The stack names are qualified: on Apple, FP declares Combine stacks with the same names.

private func vals<O: Sendable, F: Error>(_ publisher: Publisher<O, F>) async -> [O] {
    var out: [O] = []
    for await result in publisher._stream {
        if case let .success(v) = result { out.append(v) }
    }
    return out
}

// MARK: - ReaderTPublisher

@Suite(.timeLimit(.minutes(1))) struct ReaderTPublisherTests {
    @Test func mapThreadsEnvironment() async {
        let reader = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let mapped = reader.readerT.map { $0 * 2 }
        #expect(await vals(mapped.rawValue.runReader(5)) == [10])
    }

    @Test func liftA2CombinesUnderSharedEnv() async {
        let r1 = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let r2 = Reader<Int, Publisher<Int, Never>> { env in .just(env + 1) }
        let combined = ReactiveConcurrencyTransformers.ReaderTPublisher.liftA2(+)(r1.readerT, r2.readerT)
        #expect(await vals(combined.rawValue.runReader(10)) == [21])
    }

    @Test func flatMap() async {
        let reader = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let chained = reader.readerT >>- { n in Reader<Int, Publisher<Int, Never>> { env in .just(n + env) }.readerT }
        #expect(await vals(chained.rawValue.runReader(3)) == [6])
    }
}

// MARK: - PublisherTWriter

// Representation is now Publisher<Writer<W, A>, F> — the log is carried inside the effect.
@Suite(.timeLimit(.minutes(1))) struct PublisherTWriterTests {
    @Test func mapPreservesLog() async {
        let writer = Publisher<Writer<[String], Int>, Never>.just(Writer(3, ["start"]))
        let mapped = writer.publisherT.map { $0 * 2 }
        let results = await vals(mapped.rawValue)
        #expect(results.map(\.value) == [6])
        #expect(results.map(\.log) == [["start"]])
    }

    // Bind combines the outer element's log with the continuation's log (lawful WriterT).
    @Test func flatMapCombinesLogs() async {
        let writer = Publisher<Writer<[String], Int>, Never>.just(Writer(3, ["outer"]))
        let chained = writer.publisherT.flatMap { n in
            Publisher<Writer<[String], String>, Never>.just(Writer("\(n)", ["inner"])).publisherT
        }
        let results = await vals(chained.rawValue)
        #expect(results.map(\.value) == ["3"])
        #expect(results.map(\.log) == [["outer", "inner"]])
    }

    @Test func applyCombinesLogs() async {
        let wf = Publisher<Writer<[String], @Sendable (Int) -> Int>, Never>.just(Writer({ $0 + 1 }, ["f"]))
        let wa = Publisher<Writer<[String], Int>, Never>.just(Writer(10, ["a"]))
        let result = wf.publisherT <*> wa.publisherT
        let results = await vals(result.rawValue)
        #expect(results.map(\.value) == [11])
        #expect(results.map(\.log) == [["f", "a"]])
    }
}

// MARK: - StatefulTPublisher

@Suite(.timeLimit(.minutes(1))) struct StatefulTPublisherTests {
    @Test func mapThreadsState() async {
        let stateful = Stateful<Int, Publisher<Int, Never>> { s in
            s += 1
            return .just(s)
        }
        let mapped = stateful.statefulT.map { $0 * 10 }
        var state = 0
        let publisher = mapped.rawValue.run(&state)
        #expect(await vals(publisher) == [10])
        #expect(state == 1)
    }

    @Test func liftA2ThreadsStateSequentially() async {
        let bump = { @Sendable () -> Stateful<Int, Publisher<Int, Never>> in
            Stateful<Int, Publisher<Int, Never>> { s in s += 1; return .just(s) }
        }
        let combined = ReactiveConcurrencyTransformers.StatefulTPublisher.liftA2(+)(bump().statefulT, bump().statefulT)
        var state = 0
        let publisher = combined.rawValue.run(&state)
        #expect(await vals(publisher) == [3]) // (1) + (2)
        #expect(state == 2)
    }
}
