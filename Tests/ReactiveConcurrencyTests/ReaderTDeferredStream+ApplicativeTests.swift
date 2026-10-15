// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

@Suite(.timeLimit(.minutes(1))) struct ReaderTDeferredStreamApplicativeTests {
    struct Env: Sendable { let value: Int }

    // MARK: - Reader<Env, DeferredStream<A>> — Reader as outer, DeferredStream as inner

    @Test func apply() async {
        let rf: Reader<Env, DeferredStream<@Sendable (Int) -> String>> = Reader { _ in
            DeferredStream { AsyncStream { c in c.yield { "\($0)" }; c.finish() } }
        }
        let ra: Reader<Env, DeferredStream<Int>> = Reader { env in
            DeferredStream { AsyncStream { c in c.yield(env.value); c.finish() } }
        }
        let result = ReaderTDeferredStream.apply(rf.readerT, ra.readerT)
        var values: [String] = []
        for await v in result.rawValue(Env(value: 7)) {
            values.append(v)
        }
        #expect(values == ["7"])
    }

    @Test func applyUsesEnv() async {
        let rf: Reader<Env, DeferredStream<@Sendable (Int) -> Int>> = Reader { env in
            let mul = env.value
            return DeferredStream { AsyncStream { c in c.yield { $0 + mul }; c.finish() } }
        }
        let ra: Reader<Env, DeferredStream<Int>> = Reader { env in
            DeferredStream { AsyncStream { c in c.yield(env.value * 2); c.finish() } }
        }
        let result = ReaderTDeferredStream.apply(rf.readerT, ra.readerT)
        let env = Env(value: 5)
        var values: [Int] = []
        for await v in result.rawValue(env) {
            values.append(v)
        }
        #expect(values == [15])
    }

    @Test func seqRight() async {
        let lhs: Reader<Env, DeferredStream<Int>> = Reader { _ in
            DeferredStream { AsyncStream { c in c.yield(1); c.finish() } }
        }
        let rhs: Reader<Env, DeferredStream<String>> = Reader { _ in
            DeferredStream { AsyncStream { c in c.yield("hello"); c.finish() } }
        }
        let result = lhs.readerT.seqRight(rhs.readerT)
        var values: [String] = []
        for await v in result.rawValue(Env(value: 0)) {
            values.append(v)
        }
        #expect(values == ["hello"])
    }

    @Test func seqLeft() async {
        let lhs: Reader<Env, DeferredStream<Int>> = Reader { _ in
            DeferredStream { AsyncStream { c in c.yield(99); c.finish() } }
        }
        let rhs: Reader<Env, DeferredStream<String>> = Reader { _ in
            DeferredStream { AsyncStream { c in c.yield("ignored"); c.finish() } }
        }
        let result = lhs.readerT.seqLeft(rhs.readerT)
        var values: [Int] = []
        for await v in result.rawValue(Env(value: 0)) {
            values.append(v)
        }
        #expect(values == [99])
    }
}
