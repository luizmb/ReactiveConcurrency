// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

// Builds a DeferredStream<Writer<[String], T>> from a list of (value, log) pairs.
private func writerStream<T: Sendable>(
    _ elements: [(T, [String])]
) -> DeferredStream<Writer<[String], T>> {
    DeferredStream<Writer<[String], T>> {
        AsyncStream { c in
            for (value, log) in elements { c.yield(Writer(value, log)) }
            c.finish()
        }
    }
}

private func collect<T: Sendable>(_ stream: DeferredStream<Writer<[String], T>>) async -> [Writer<[String], T>] {
    var out: [Writer<[String], T>] = []
    for await w in stream { out.append(w) }
    return out
}

@Suite(.timeLimit(.minutes(1))) struct WriterTDeferredStreamTests {
    // MARK: - DeferredStream<Writer<W, A>> — WriterT over DeferredStream (log carried inside the effect)

    @Test func mapT() async {
        let w = writerStream([(1, ["log"]), (2, ["log"])])
        let mapped = w.mapT { $0 * 3 }
        let results = await collect(mapped)
        #expect(results.map(\.value) == [3, 6])
        #expect(results.map(\.log) == [["log"], ["log"]])
    }

    // Bind now combines each element's log with the continuation's log (lawful WriterT).
    @Test func flatMapTCombinesLogs() async {
        let w = writerStream([(5, ["outer"])])
        let result = w.flatMapT { (n: Int) in writerStream([("\(n)", ["inner"])]) }
        let results = await collect(result)
        #expect(results.map(\.value) == ["5"])
        #expect(results.map(\.log) == [["outer", "inner"]])
    }

    @Test func applicativeLogsAccumulate() async {
        let wf = writerStream([({ (n: Int) in "\(n)" } as @Sendable (Int) -> String, ["fn"])])
        let wa = writerStream([(7, ["val"])])
        let result = applyWriterDeferredStream(wf, wa)
        let results = await collect(result)
        #expect(results.map(\.value) == ["7"])
        #expect(results.map(\.log) == [["fn", "val"]])
    }

    @Test func seqRight() async {
        let lhs = writerStream([(1, ["a"])])
        let rhs = writerStream([("hello", ["b"])])
        let results = await collect(seqRightWriterDeferredStream(lhs, rhs))
        #expect(results.map(\.value) == ["hello"])
        #expect(results.map(\.log) == [["a", "b"]])
    }

    @Test func seqLeft() async {
        let lhs = writerStream([(99, ["a"])])
        let rhs = writerStream([("ignored", ["b"])])
        let results = await collect(seqLeftWriterDeferredStream(lhs, rhs))
        #expect(results.map(\.value) == [99])
        #expect(results.map(\.log) == [["a", "b"]])
    }
}
