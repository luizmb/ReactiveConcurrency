// SPDX-License-Identifier: Apache-2.0

import Foundation
@testable import ReactiveConcurrency
import Testing

// Sendable pair so combineLatest tuples can be collected and projected in assertions.
private struct Pair: Sendable {
    let first: Int
    let second: Int
}

// Lets pending consumer Tasks register/run before we send into a hot subject.
private func settle() async {
    for _ in 0..<20 {
        await Task.yield()
    }
}

// Polls a condition instead of sleeping a fixed amount — merge/combineLatest deliver via
// async Tasks that can be scheduled late on CI runners, making fixed sleeps flaky.
private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    for _ in 0..<(timeoutMs / 2) {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

@Suite(.timeLimit(.minutes(1))) struct SequenceOperatorTests {
    @Test func scanAccumulates() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...4).scan(0, +)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [1, 3, 6, 10])
    }

    @Test func reduceEmitsFinalValue() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...4).reduce(0, +)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [10])
    }

    @Test func collectBuffersAll() async {
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence(1...3).collect()._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [[1, 2, 3]])
    }

    @Test func prependAddsBeforeUpstream() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(3...4).prepend(1, 2)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3, 4])
    }

    @Test func appendAddsAfterUpstream() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...2).append(3, 4)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3, 4])
    }
}

@Suite(.timeLimit(.minutes(1))) struct CombineOperatorTests {
    @Test func mergeInterleaves() async {
        let values = Collector<Int>()
        let a = Publisher<Int, Never>.sequence([1, 3])
        let b = Publisher<Int, Never>.sequence([2, 4])
        let cancellable = a.merge(with: b).sink { values.append($0) }
        await poll { values.values.count == 4 }
        cancellable.cancel()
        #expect(values.values.sorted() == [1, 2, 3, 4])
    }

    @Test func zipPairsElements() async {
        var result: [(Int, String)] = []
        let ints = Publisher<Int, Never>.sequence(1...3)
        let strs = Publisher<String, Never>.sequence(["a", "b", "c"])
        for await r in ints.zip(strs)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result.map(\.0) == [1, 2, 3])
        #expect(result.map(\.1) == ["a", "b", "c"])
    }

    @Test func zipStopsAtShorterPublisher() async {
        var result: [(Int, Int)] = []
        let a = Publisher<Int, Never>.sequence(1...5)
        let b = Publisher<Int, Never>.sequence(1...3)
        for await r in a.zip(b)._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result.count == 3)
    }

    // Regression (A1): the old left-driven zip only polled `other` after `self` emitted, so a
    // failure on `other` while `self` was silent was never observed and downstream hung.
    @Test func zipForwardsFailureFromOtherWhileSelfSilent() async {
        enum E: Error, Sendable, Equatable { case boom }
        let silent = PassthroughSubject<Int, E>()
        let completions = Collector<Subscribers.Completion<E>>()
        let values = Collector<Int>()

        let sub = silent.eraseToPublisher()
            .zip(Publisher<Int, E> { c in c.fail(.boom) })
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { values.append($0.0) })

        await poll { completions.values.count == 1 }
        sub.cancel()

        #expect(values.values.isEmpty)
        #expect(completions.values == [.failure(.boom)])
    }

    // Regression (A1): `other` finishes empty while `self` is silent → the pair can never form,
    // so zip must complete rather than hang.
    @Test func zipCompletesWhenOtherFinishesEmptyWhileSelfSilent() async {
        let silent = PassthroughSubject<Int, Never>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let sub = silent.eraseToPublisher()
            .zip(Publisher<Int, Never>.empty())
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { _ in })

        await poll { completions.values.count == 1 }
        sub.cancel()

        #expect(completions.values == [.finished])
    }

    // Pairwise order is preserved even when the two sides interleave arbitrarily.
    @Test func zipPreservesPairOrderUnderInterleaving() async {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let result = Collector<Pair>()
        let sub = a.eraseToPublisher()
            .zip(b.eraseToPublisher())
            .map { Pair(first: $0.0, second: $0.1) }
            .sink { result.append($0) }

        await settle()
        a.send(1); a.send(2) // buffered, no partner yet
        await settle()
        b.send(10) // → (1, 10)
        await poll { result.values.count >= 1 }
        b.send(20) // → (2, 20)
        await poll { result.values.count >= 2 }
        b.send(30); a.send(3) // → (3, 30), regardless of arrival order
        await poll { result.values.count >= 3 }
        sub.cancel()

        #expect(result.values.map(\.first) == [1, 2, 3])
        #expect(result.values.map(\.second) == [10, 20, 30])
    }

    @Test func combineLatestEmitsOnEachUpdate() async {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<String, Never>()
        let values = Collector<String>()

        let cancellable = subject1.eraseToPublisher()
            .combineLatest(subject2.eraseToPublisher())
            .map { "\($0.0)\($0.1)" }
            .sink { values.append($0) }

        await settle()
        subject1.send(1) // → latestA=1, no b yet
        await settle()
        subject2.send("a") // → latestB="a", emit (1,"a")
        await poll { values.values.count >= 1 }
        subject1.send(2) // → latestA=2, emit (2,"a")
        await poll { values.values.count >= 2 }
        subject2.send("b") // → latestB="b", emit (2,"b")
        await poll { values.values.count >= 3 }
        cancellable.cancel()

        #expect(values.values == ["1a", "2a", "2b"])
    }

    // One source closes without ever emitting → sequence must terminate immediately,
    // not drain the other source forever.
    @Test func combineLatestTerminatesWhenOneSourceNeverEmits() async {
        let completions = Collector<Subscribers.Completion<Never>>()

        let empty = Publisher<Int, Never>.empty()
        let infinite = PassthroughSubject<Int, Never>()

        let cancellable = empty
            .combineLatest(infinite.eraseToPublisher())
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { _ in })

        await poll { completions.values.count == 1 }
        cancellable.cancel()

        #expect(completions.values == [.finished])
    }

    // One source closes after emitting → combineLatest continues pairing with the last
    // known value from the finished source until the other source also closes.
    @Test func combineLatestContinuesAfterOneSourceFinishes() async {
        // a emits 1 then finishes; b then emits 10, 20, 30. combineLatest must keep pairing
        // with a's last value (1) after a has finished. Driven via subjects so the
        // interleaving is controlled — feeding two cold publishers concurrently would race.
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let result = Collector<Pair>()
        let sub = a.eraseToPublisher()
            .combineLatest(b.eraseToPublisher())
            .map { Pair(first: $0.0, second: $0.1) }
            .sink { result.append($0) }

        await settle()
        a.send(1)
        a.send(completion: .finished)
        await settle()
        b.send(10)
        await poll { result.values.count >= 1 }
        b.send(20)
        await poll { result.values.count >= 2 }
        b.send(30)
        await poll { result.values.count >= 3 }
        sub.cancel()

        #expect(result.values.map(\.first) == [1, 1, 1])
        #expect(result.values.map(\.second) == [10, 20, 30])
    }

    // Error from either source propagates and terminates the sequence.
    @Test func combineLatestPropagatesErrorFromFirstSource() async {
        enum E: Error, Sendable, Equatable { case boom }
        let values = Collector<(Int, Int)>()
        let completions = Collector<Subscribers.Completion<E>>()

        let failing: Publisher<Int, E> = Publisher { c in c.fail(.boom) }
        let other = Publisher<Int, E>.just(1).mapError { $0 }

        let cancellable = failing
            .combineLatest(other)
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { values.append($0) })

        await poll { completions.values.count == 1 }
        cancellable.cancel()

        #expect(values.values.isEmpty)
        #expect(completions.values == [.failure(.boom)])
    }
}

@Suite(.timeLimit(.minutes(1))) struct ErrorOperatorTests {
    @Test func catchReplacesFailureWithRecovery() async {
        enum E: Error, Sendable { case boom }
        let values = Collector<Int>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let pub: Publisher<Int, E> = Publisher { c in
            c.yield(1)
            c.fail(.boom)
        }

        let cancellable = pub
            .catch { _ in Publisher<Int, Never>.just(99) }
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { values.append($0) })
        await poll { values.values.count == 2 && completions.values.count == 1 }
        cancellable.cancel()

        #expect(values.values == [1, 99])
        #expect(completions.values == [.finished])
    }

    @Test func replaceErrorSubstitutesValue() async {
        enum E: Error, Sendable { case boom }
        let values = Collector<Int>()

        let pub: Publisher<Int, E> = Publisher { c in
            c.yield(1)
            c.fail(.boom)
        }

        let cancellable = pub.replaceError(with: 0)
            .sink { values.append($0) }
        await poll { values.values.count == 2 }
        cancellable.cancel()

        #expect(values.values == [1, 0])
    }

    @Test func retryRepeatsOnFailure() async {
        enum E: Error, Sendable { case boom }
        let counter = AtomicCounter()

        let pub = Publisher<Int, E> { c in
            let n = counter.increment()
            if n < 3 {
                c.yield(n)
                c.fail(.boom)
            } else {
                c.yield(n)
            }
        }

        let values = Collector<Int>()
        let completions = Collector<Subscribers.Completion<E>>()
        let cancellable = pub.retry(2)
            .sink(receiveCompletion: { completions.append($0) }, receiveValue: { values.append($0) })
        await poll { values.values.count == 3 && completions.values.count == 1 }
        cancellable.cancel()

        #expect(values.values == [1, 2, 3])
        #expect(completions.values == [.finished])
    }
}

@Suite(.timeLimit(.minutes(1))) struct FlatMapTests {
    @Test func flatMapUnboundedMergesAllInner() async {
        let stream = Publisher<Int, Never>.sequence(1...3)
            .flatMap { n in Publisher<Int, Never>.sequence([n * 10, n * 10 + 1]) }
            ._stream
        var result: [Int] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result.sorted() == [10, 11, 20, 21, 30, 31])
    }

    @Test func flatMapMaxPublishersRunsSequentiallyWhenOne() async {
        // maxPublishers: 1 serialises inner publishers; output order matches source order
        let stream = Publisher<Int, Never>.sequence(1...3)
            .flatMap(maxPublishers: 1) { n in Publisher<Int, Never>.sequence([n * 10, n * 10 + 1]) }
            ._stream
        var result: [Int] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [10, 11, 20, 21, 30, 31])
    }

    // Regression (A5): when the outer completes, switchToLatest must let the still-running latest
    // inner finish delivering — Combine completes only after the outer AND the last inner complete.
    // The old code cancelled the inner on outer completion, silently dropping its in-flight values.
    @Test func switchToLatestDeliversLastInnerAfterOuterFinishes() async {
        let outer = PassthroughSubject<Publisher<Int, Never>, Never>()
        let values = Collector<Int>()
        let sub = outer.eraseToPublisher()
            .switchToLatest()
            .sink { values.append($0) }

        await settle()
        outer.send(Publisher<Int, Never>.sequence(1...3))
        outer.send(completion: .finished) // outer completes while the inner is still delivering
        await poll { values.values.count >= 3 }
        sub.cancel()

        #expect(values.values == [1, 2, 3])
    }
}

@Suite(.timeLimit(.minutes(1))) struct CombineTransformTests {
    @Test func zipWithTransformAppliesClosure() async {
        let stream = Publisher<Int, Never>.sequence(1...3)
            .zip(Publisher<Int, Never>.sequence(10...12)) { a, b in "\(a)+\(b)" }
            ._stream
        var result: [String] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == ["1+10", "2+11", "3+12"])
    }

    @Test func combineLatestWithTransformAppliesClosure() async {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let values = Collector<String>()

        let sub = a.eraseToPublisher()
            .combineLatest(b.eraseToPublisher()) { x, y in "\(x)-\(y)" }
            .sink { values.append($0) }

        await settle()
        a.send(1) // latestA=1, no emit yet (b unseen)
        await settle()
        b.send(2) // emit "1-2"
        await poll { values.values.count >= 1 }
        a.send(3) // emit "3-2"
        await poll { values.values.count >= 2 }
        sub.cancel()

        #expect(values.values == ["1-2", "3-2"])
    }
}

@Suite(.timeLimit(.minutes(1))) struct TryFilteringTests {
    enum TestError: Error, Equatable { case bad }

    @Test func tryFirstWhereThrowsPropagatesError() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5)
            .tryFirst(where: { v throws(TestError) in
                if v == 2 { throw TestError.bad }
                return v > 3
            })._stream {
            switch r {
            case let .success(v): result.append(v)
            case .failure: result.append(-1)
            }
        }
        #expect(result == [-1])
    }

    @Test func tryLastWhereReturnsLastMatch() async {
        let stream = Publisher<Int, Never>.sequence(1...5)
            .tryLast(where: { v throws(TestError) in v.isMultiple(of: 2) })._stream
        var result: [Int] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [4])
    }

    @Test func tryDropWhileThrowsPropagatesError() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5)
            .tryDrop(while: { v throws(TestError) in
                if v == 3 { throw TestError.bad }
                return v < 3
            })._stream {
            switch r {
            case let .success(v): result.append(v)
            case .failure: result.append(-1)
            }
        }
        #expect(result == [-1])
    }

    @Test func tryPrefixWhileThrowsPropagatesError() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5)
            .tryPrefix(while: { v throws(TestError) in
                if v == 3 { throw TestError.bad }
                return v < 4
            })._stream {
            switch r {
            case let .success(v): result.append(v)
            case .failure: result.append(-1)
            }
        }
        #expect(result == [1, 2, -1])
    }

    @Test func tryContainsWhereFindsMatch() async {
        let stream = Publisher<Int, Never>.sequence(1...5)
            .tryContains(where: { v throws(TestError) in v == 4 })._stream
        var result: [Bool] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [true])
    }

    @Test func tryAllSatisfyTrueWhenAllMatch() async {
        let stream = Publisher<Int, Never>.sequence(1...5)
            .tryAllSatisfy { v throws(TestError) in v < 10 }._stream
        var result: [Bool] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [true])
    }

    @Test func tryRemoveDuplicatesByPredicate() async {
        let stream = Publisher<Int, Never>.sequence([1, 2, 5, 6, 10])
            .tryRemoveDuplicates(by: { a, b throws(TestError) in abs(a - b) < 3 })._stream
        var result: [Int] = []
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [1, 5, 10])
    }
}
