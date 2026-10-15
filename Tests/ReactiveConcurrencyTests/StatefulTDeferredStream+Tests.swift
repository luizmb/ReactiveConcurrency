// SPDX-License-Identifier: Apache-2.0

import DataStructure
import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

@Suite(.timeLimit(.minutes(1))) struct StatefulTDeferredStreamTests {
    // MARK: - Stateful<S, DeferredStream<A>> — State as outer, DeferredStream as inner

    // Applicative-only: the state is threaded outside the effect, so there is no lawful monad.

    @Test func mapTransformsValues() async {
        let s = Stateful<Int, DeferredStream<Int>> { _ in
            DeferredStream { AsyncStream { c in
                c.yield(1)
                c.yield(2)
                c.yield(3)
                c.finish()
            }
            }
        }
        let mapped = s.statefulT.map { $0 * 10 }
        var results: [Int] = []
        for await value in mapped.rawValue.eval(0) {
            results.append(value)
        }
        #expect(results == [10, 20, 30])
    }

    @Test func mapPreservesState() async {
        let s = Stateful<Int, DeferredStream<Int>> { state in
            let v = state
            state += 1
            return DeferredStream { AsyncStream { c in
                c.yield(v)
                c.finish()
            }
            }
        }
        let mapped = s.statefulT.map { $0 + 100 }
        let (stream, finalState) = mapped.rawValue.runStateful(7)
        var results: [Int] = []
        for await value in stream {
            results.append(value)
        }
        #expect(results == [107])
        #expect(finalState == 8)
    }

    @Test func applicativeZipsElements() async {
        let sf = Stateful<Int, DeferredStream<@Sendable (Int) -> String>> { _ in
            DeferredStream { AsyncStream { c in
                c.yield { "\($0)" }
                c.finish()
            }
            }
        }
        let sa = Stateful<Int, DeferredStream<Int>> { _ in
            DeferredStream { AsyncStream { c in
                c.yield(42)
                c.finish()
            }
            }
        }
        let result = StatefulTDeferredStream.apply(sf.statefulT, sa.statefulT)
        var results: [String] = []
        for await value in result.rawValue.eval(0) {
            results.append(value)
        }
        #expect(results == ["42"])
    }

    @Test func seqRight() async {
        let lhs = Stateful<Int, DeferredStream<Int>> { _ in
            DeferredStream { AsyncStream { c in c.yield(1); c.finish() } }
        }
        let rhs = Stateful<Int, DeferredStream<String>> { _ in
            DeferredStream { AsyncStream { c in c.yield("hello"); c.finish() } }
        }
        let result = lhs.statefulT.seqRight(rhs.statefulT)
        var results: [String] = []
        for await value in result.rawValue.eval(0) {
            results.append(value)
        }
        #expect(results == ["hello"])
    }

    @Test func seqLeft() async {
        let lhs = Stateful<Int, DeferredStream<Int>> { _ in
            DeferredStream { AsyncStream { c in c.yield(99); c.finish() } }
        }
        let rhs = Stateful<Int, DeferredStream<String>> { _ in
            DeferredStream { AsyncStream { c in c.yield("ignored"); c.finish() } }
        }
        let result = lhs.statefulT.seqLeft(rhs.statefulT)
        var results: [Int] = []
        for await value in result.rawValue.eval(0) {
            results.append(value)
        }
        #expect(results == [99])
    }
}
