// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import DataStructureOperators
import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private func writerStream<T: Sendable>(
    _ elements: [(T, [String])]
) -> DeferredStream<Writer<[String], T>> {
    DeferredStream<Writer<[String], T>> {
        AsyncStream { c in
            for (value, log) in elements {
                c.yield(Writer(value, log))
            }
            c.finish()
        }
    }
}

private func collect<T: Sendable>(_ stream: DeferredStream<Writer<[String], T>>) async -> [Writer<[String], T>] {
    var out: [Writer<[String], T>] = []
    for await w in stream {
        out.append(w)
    }
    return out
}

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTWriterOperatorsTests {
    @Test func fmap() async {
        let w = writerStream([(1, ["log"]), (2, ["log"])])
        let results = await collect({ $0 * 3 } <£^> w)
        #expect(results.map(\.value) == [3, 6])
        #expect(results.map(\.log) == [["log"], ["log"]])
    }

    @Test func flippedFmap() async {
        let w = writerStream([(1, ["log"]), (2, ["log"])])
        let results = await collect(w <&^> { $0 * 3 })
        #expect(results.map(\.value) == [3, 6])
        #expect(results.map(\.log) == [["log"], ["log"]])
    }

    // Bind combines logs (lawful WriterT): outer <> inner.
    @Test func bind() async {
        let w = writerStream([(4, ["outer"])])
        let result = w >>- { (n: Int) in writerStream([("\(n * 2)", ["inner"])]) }
        let results = await collect(result)
        #expect(results.map(\.value) == ["8"])
        #expect(results.map(\.log) == [["outer", "inner"]])
    }

    @Test func apply() async {
        let wf = writerStream([({ (n: Int) in "\(n)" } as @Sendable (Int) -> String, ["fn"])])
        let wa = writerStream([(7, ["val"])])
        let results = await collect(wf <*> wa)
        #expect(results.map(\.value) == ["7"])
        #expect(results.map(\.log) == [["fn", "val"]])
    }

    @Test func seqRight() async {
        let lhs = writerStream([(1, ["a"])])
        let rhs = writerStream([("hello", ["b"])])
        let results = await collect(lhs *> rhs)
        #expect(results.map(\.value) == ["hello"])
        #expect(results.map(\.log) == [["a", "b"]])
    }

    @Test func seqLeft() async {
        let lhs = writerStream([(99, ["a"])])
        let rhs = writerStream([("ignored", ["b"])])
        let results = await collect(lhs <* rhs)
        #expect(results.map(\.value) == [99])
        #expect(results.map(\.log) == [["a", "b"]])
    }
}
