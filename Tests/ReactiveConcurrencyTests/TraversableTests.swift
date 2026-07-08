// SPDX-License-Identifier: Apache-2.0

// Traversals: [Effect<A>] -> Effect<[A]> (sequence) and [A] -> (A -> Effect<B>) -> Effect<[B]>.

@testable import ReactiveConcurrency
import Testing

private func collectS<A: Sendable>(_ stream: DeferredStream<A>) async -> [A] {
    var out: [A] = []
    for await x in stream {
        out.append(x)
    }
    return out
}

private func collectP<A: Sendable>(_ publisher: Publisher<A, Never>) async -> [A] {
    var out: [A] = []
    for await result in publisher._stream {
        if case let .success(v) = result { out.append(v) }
    }
    return out
}

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskTraversableTests {
    @Test func sequenceRunsInOrder() async {
        let tasks = [DeferredTask { 1 }, DeferredTask { 2 }, DeferredTask { 3 }]
        #expect(await sequenceDeferredTask(tasks).run() == [1, 2, 3])
    }

    @Test func sequenceEmptyIsEmpty() async {
        #expect(await sequenceDeferredTask([DeferredTask<Int>]()).run().isEmpty)
    }

    @Test func traverseMapsEach() async {
        let result = await traverseDeferredTask([1, 2, 3]) { n in DeferredTask { n * 10 } }.run()
        #expect(result == [10, 20, 30])
    }

    @Test func traverseOptionalSome() async {
        let result = await traverseDeferredTask(Optional(5)) { n in DeferredTask { n + 1 } }.run()
        #expect(result == 6)
    }

    @Test func traverseOptionalNone() async {
        let result = await traverseDeferredTask(Int?.none) { n in DeferredTask { n + 1 } }.run()
        #expect(result == nil)
    }
}

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamTraversableTests {
    private func streamOf(_ xs: [Int]) -> DeferredStream<Int> {
        DeferredStream<Int> {
            AsyncStream { c in
                for x in xs {
                    c.yield(x)
                }
                c.finish()
            }
        }
    }

    // Zippy: positional pairing, truncates to the shortest stream.
    @Test func sequenceZipsPositionally() async {
        let result = await collectS(sequenceDeferredStream([streamOf([1, 2, 3]), streamOf([10, 20, 30])]))
        #expect(result == [[1, 10], [2, 20], [3, 30]])
    }

    @Test func sequenceTruncatesToShortest() async {
        let result = await collectS(sequenceDeferredStream([streamOf([1, 2, 3]), streamOf([10])]))
        #expect(result == [[1, 10]])
    }

    @Test func traverseMapsEach() async {
        let result = await collectS(traverseDeferredStream([1, 2]) { n in streamOf([n, n * 10]) })
        #expect(result == [[1, 2], [10, 20]])
    }
}

@Suite(.timeLimit(.minutes(1))) struct PublisherTraversableTests {
    @Test func sequenceZipsPositionally() async {
        let result = await collectP(sequencePublisher([
            Publisher<Int, Never>.sequence([1, 2, 3]),
            Publisher<Int, Never>.sequence([10, 20, 30]),
        ]))
        #expect(result == [[1, 10], [2, 20], [3, 30]])
    }

    @Test func sequenceTruncatesToShortest() async {
        let result = await collectP(sequencePublisher([
            Publisher<Int, Never>.sequence([1, 2, 3]),
            Publisher<Int, Never>.sequence([10]),
        ]))
        #expect(result == [[1, 10]])
    }

    @Test func traverseMapsEach() async {
        let result = await collectP(traversePublisher([1, 2]) { n in Publisher<Int, Never>.sequence([n, n * 10]) })
        #expect(result == [[1, 2], [10, 20]])
    }
}
