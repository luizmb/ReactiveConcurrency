// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTWriterTests {
    // MARK: - DeferredTask<Writer<W, A>> — WriterT over DeferredTask (log carried inside the effect)

    @Test func map() async {
        let w = DeferredTask { Writer<[String], Int>(5, ["log"]) }
        let mapped = w.deferredTaskT.map { $0 * 2 }
        let result = await mapped.rawValue.run()
        #expect(result.value == 10)
        #expect(result.log == ["log"])
    }

    // Bind now combines the continuation's log with the outer log (lawful WriterT),
    // whereas the old Writer<W, DeferredTask<A>> shape discarded the continuation's log.
    @Test func flatMapCombinesLogs() async {
        let w = DeferredTask { Writer<[String], Int>(3, ["outer"]) }
        let result = w.deferredTaskT.flatMap { n in
            DeferredTask { Writer<[String], String>("\(n * 2)", ["inner"]) }.deferredTaskT
        }
        let value = await result.rawValue.run()
        #expect(value.value == "6")
        #expect(value.log == ["outer", "inner"])
    }

    @Test func flatMapChains() async {
        let w = DeferredTask { Writer<[String], Int>(10, ["a"]) }
        let step2: @Sendable (Int) -> DeferredTaskTWriter<[String], Int> = { n in
            DeferredTask { Writer(n + 5, ["b"]) }.deferredTaskT
        }
        let result = w.deferredTaskT.flatMap(step2)
        let value = await result.rawValue.run()
        #expect(value.value == 15)
        #expect(value.log == ["a", "b"])
    }

    @Test func applicativeLogsAccumulate() async {
        let wf = DeferredTask { Writer<[String], @Sendable (Int) -> String>({ "\($0)" }, ["fn"]) }
        let wa = DeferredTask { Writer<[String], Int>(42, ["val"]) }
        let result = DeferredTaskTWriter.apply(wf.deferredTaskT, wa.deferredTaskT)
        let value = await result.rawValue.run()
        #expect(value.value == "42")
        #expect(value.log == ["fn", "val"])
    }

    @Test func seqRight() async {
        let lhs = DeferredTask { Writer<[String], Int>(1, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("hello", ["b"]) }
        let result = lhs.deferredTaskT.seqRight(rhs.deferredTaskT)
        let value = await result.rawValue.run()
        #expect(value.value == "hello")
        #expect(value.log == ["a", "b"])
    }

    @Test func seqLeft() async {
        let lhs = DeferredTask { Writer<[String], Int>(99, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("ignored", ["b"]) }
        let result = lhs.deferredTaskT.seqLeft(rhs.deferredTaskT)
        let value = await result.rawValue.run()
        #expect(value.value == 99)
        #expect(value.log == ["a", "b"])
    }
}
