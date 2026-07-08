// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private func vals<O: Sendable, F: Error>(_ publisher: Publisher<O, F>) async -> [O] {
    var out: [O] = []
    for await result in publisher._stream {
        if case let .success(v) = result { out.append(v) }
    }
    return out
}

// MARK: - ReaderTPublisher

@Suite(.timeLimit(.minutes(1))) struct ReaderTPublisherTests {
    @Test func mapTThreadsEnvironment() async {
        let reader = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let mapped = reader.mapT { $0 * 2 }
        #expect(await vals(mapped.runReader(5)) == [10])
    }

    @Test func liftA2CombinesUnderSharedEnv() async {
        let r1 = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let r2 = Reader<Int, Publisher<Int, Never>> { env in .just(env + 1) }
        let combined = liftA2ReaderPublisher(+)(r1, r2)
        #expect(await vals(combined.runReader(10)) == [21])
    }

    @Test func flatMapT() async {
        let reader = Reader<Int, Publisher<Int, Never>> { env in .just(env) }
        let chained = reader >>- { n in Reader<Int, Publisher<Int, Never>> { env in .just(n + env) } }
        #expect(await vals(chained.runReader(3)) == [6])
    }
}

// MARK: - WriterTPublisher

// Representation is now Publisher<Writer<W, A>, F> — the log is carried inside the effect.
@Suite(.timeLimit(.minutes(1))) struct WriterTPublisherTests {
    @Test func mapTPreservesLog() async {
        let writer = Publisher<Writer<[String], Int>, Never>.just(Writer(3, ["start"]))
        let mapped = writer.mapT { $0 * 2 }
        let results = await vals(mapped)
        #expect(results.map(\.value) == [6])
        #expect(results.map(\.log) == [["start"]])
    }

    // Bind combines the outer element's log with the continuation's log (lawful WriterT).
    @Test func flatMapTCombinesLogs() async {
        let writer = Publisher<Writer<[String], Int>, Never>.just(Writer(3, ["outer"]))
        let chained = writer.flatMapT { (n: Int) in
            Publisher<Writer<[String], String>, Never>.just(Writer("\(n)", ["inner"]))
        }
        let results = await vals(chained)
        #expect(results.map(\.value) == ["3"])
        #expect(results.map(\.log) == [["outer", "inner"]])
    }

    @Test func applyCombinesLogs() async {
        let wf = Publisher<Writer<[String], @Sendable (Int) -> Int>, Never>.just(Writer({ $0 + 1 }, ["f"]))
        let wa = Publisher<Writer<[String], Int>, Never>.just(Writer(10, ["a"]))
        let result = applyWriterPublisher(wf, wa)
        let results = await vals(result)
        #expect(results.map(\.value) == [11])
        #expect(results.map(\.log) == [["f", "a"]])
    }
}

// MARK: - StatefulTPublisher

@Suite(.timeLimit(.minutes(1))) struct StatefulTPublisherTests {
    @Test func mapTThreadsState() async {
        let stateful = Stateful<Int, Publisher<Int, Never>> { s in
            s += 1
            return .just(s)
        }
        let mapped = stateful.mapT { $0 * 10 }
        var state = 0
        let publisher = mapped.run(&state)
        #expect(await vals(publisher) == [10])
        #expect(state == 1)
    }

    @Test func liftA2ThreadsStateSequentially() async {
        let bump = { @Sendable () -> Stateful<Int, Publisher<Int, Never>> in
            Stateful<Int, Publisher<Int, Never>> { s in s += 1; return .just(s) }
        }
        let combined = liftA2StatefulPublisher(+)(bump(), bump())
        var state = 0
        let publisher = combined.run(&state)
        #expect(await vals(publisher) == [3]) // (1) + (2)
        #expect(state == 2)
    }
}
