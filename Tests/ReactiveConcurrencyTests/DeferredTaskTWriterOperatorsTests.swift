// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import DataStructure
import DataStructureOperators
import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTWriterOperatorsTests {
    @Test func fmap() async {
        let w = DeferredTask { Writer<[String], Int>(5, ["log"]) }
        let result = { $0 * 2 } <£^> w
        let value = await result.run()
        #expect(value.value == 10)
        #expect(value.log == ["log"])
    }

    @Test func flippedFmap() async {
        let w = DeferredTask { Writer<[String], Int>(5, ["log"]) }
        let result = w <&^> { $0 * 2 }
        let value = await result.run()
        #expect(value.value == 10)
        #expect(value.log == ["log"])
    }

    // Bind combines logs (lawful WriterT): outer <> inner.
    @Test func bind() async {
        let w = DeferredTask { Writer<[String], Int>(3, ["outer"]) }
        // Annotate the parameter so the WriterT bind (a -> …) is selected over the plain
        // DeferredTask bind (Writer<w, a> -> …); both are in scope for DeferredTask<Writer<…>>.
        let result = w >>- { (n: Int) in
            DeferredTask { Writer<[String], String>("\(n)", ["inner"]) }
        }
        let value = await result.run()
        #expect(value.value == "3")
        #expect(value.log == ["outer", "inner"])
    }

    @Test func kleisli() async {
        let f: @Sendable (Int) -> DeferredTask<Writer<[String], Int>> = { n in DeferredTask { Writer(n + 1, ["f"]) } }
        let g: @Sendable (Int) -> DeferredTask<Writer<[String], String>> = { n in DeferredTask { Writer("\(n)", ["g"]) } }
        let result = (f >=> g)(4)
        let value = await result.run()
        #expect(value.value == "5")
        #expect(value.log == ["f", "g"])
    }

    @Test func apply() async {
        let wf = DeferredTask { Writer<[String], @Sendable (Int) -> String>({ "\($0)" }, ["fn"]) }
        let wa = DeferredTask { Writer<[String], Int>(42, ["val"]) }
        let result = wf <*> wa
        let value = await result.run()
        #expect(value.value == "42")
        #expect(value.log == ["fn", "val"])
    }

    @Test func seqRight() async {
        let lhs = DeferredTask { Writer<[String], Int>(1, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("hello", ["b"]) }
        let result = lhs *> rhs
        let value = await result.run()
        #expect(value.value == "hello")
        #expect(value.log == ["a", "b"])
    }

    @Test func seqLeft() async {
        let lhs = DeferredTask { Writer<[String], Int>(99, ["a"]) }
        let rhs = DeferredTask { Writer<[String], String>("ignored", ["b"]) }
        let result = lhs <* rhs
        let value = await result.run()
        #expect(value.value == 99)
        #expect(value.log == ["a", "b"])
    }
}
