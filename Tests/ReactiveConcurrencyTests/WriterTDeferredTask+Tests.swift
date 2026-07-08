// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

@Suite(.timeLimit(.minutes(1))) struct WriterTDeferredTaskTests {
    // MARK: - DeferredTask<Writer<W, A>> — WriterT over DeferredTask (log carried inside the effect)

    @Test func mapT() async {
        let w = DeferredTask { Writer<[String], Int>(5, ["log"]) }
        let mapped = w.mapT { $0 * 2 }
        let result = await mapped.run()
        #expect(result.value == 10)
        #expect(result.log == ["log"])
    }

    // Bind now combines the continuation's log with the outer log (lawful WriterT),
    // whereas the old Writer<W, DeferredTask<A>> shape discarded the continuation's log.
    @Test func flatMapTCombinesLogs() async {
        let w = DeferredTask { Writer<[String], Int>(3, ["outer"]) }
        let result = w.flatMapT { n in
            DeferredTask { Writer<[String], String>("\(n * 2)", ["inner"]) }
        }
        let value = await result.run()
        #expect(value.value == "6")
        #expect(value.log == ["outer", "inner"])
    }

    @Test func flatMapTChains() async {
        let w = DeferredTask { Writer<[String], Int>(10, ["a"]) }
        let step2: @Sendable (Int) -> DeferredTask<Writer<[String], Int>> = { n in
            DeferredTask { Writer(n + 5, ["b"]) }
        }
        let result = w.flatMapT(step2)
        let value = await result.run()
        #expect(value.value == 15)
        #expect(value.log == ["a", "b"])
    }

    @Test func applicativeLogsAccumulate() async {
        let wf = DeferredTask { Writer<[String], @Sendable (Int) -> String>({ "\($0)" }, ["fn"]) }
        let wa = DeferredTask { Writer<[String], Int>(42, ["val"]) }
        let result = applyWriterDeferredTask(wf, wa)
        let value = await result.run()
        #expect(value.value == "42")
        #expect(value.log == ["fn", "val"])
    }

    @Test func seqRight() async {
        let lhs = DeferredTask { Writer<[String], Int>(1, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("hello", ["b"]) }
        let result = seqRightWriterDeferredTask(lhs, rhs)
        let value = await result.run()
        #expect(value.value == "hello")
        #expect(value.log == ["a", "b"])
    }

    @Test func seqLeft() async {
        let lhs = DeferredTask { Writer<[String], Int>(99, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("ignored", ["b"]) }
        let result = seqLeftWriterDeferredTask(lhs, rhs)
        let value = await result.run()
        #expect(value.value == 99)
        #expect(value.log == ["a", "b"])
    }
}
