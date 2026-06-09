import Foundation
import Testing
@testable import LongLiveCombine

@Suite struct SequenceOperatorTests {
    @Test func scanAccumulates() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...4).scan(0, +)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 3, 6, 10])
    }

    @Test func reduceEmitsFinalValue() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...4).reduce(0, +)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [10])
    }

    @Test func collectBuffersAll() async {
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence(1...3).collect()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [[1, 2, 3]])
    }

    @Test func prependAddsBeforeUpstream() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(3...4).prepend(1, 2)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3, 4])
    }

    @Test func appendAddsAfterUpstream() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...2).append(3, 4)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3, 4])
    }
}

@Suite struct CombineOperatorTests {
    @Test func mergeInterleaves() async {
        let values = Collector<Int>()
        let a = Publisher<Int, Never>.sequence([1, 3])
        let b = Publisher<Int, Never>.sequence([2, 4])
        let cancellable = a.merge(with: b).sink { values.append($0) }
        try? await Task.sleep(nanoseconds: 20_000_000)
        cancellable.cancel()
        #expect(values.values.sorted() == [1, 2, 3, 4])
    }

    @Test func zipPairsElements() async {
        var result: [(Int, String)] = []
        let ints = Publisher<Int, Never>.sequence(1...3)
        let strs = Publisher<String, Never>.sequence(["a", "b", "c"])
        for await r in ints.zip(strs)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.map(\.0) == [1, 2, 3])
        #expect(result.map(\.1) == ["a", "b", "c"])
    }

    @Test func zipStopsAtShorterPublisher() async {
        var result: [(Int, Int)] = []
        let a = Publisher<Int, Never>.sequence(1...5)
        let b = Publisher<Int, Never>.sequence(1...3)
        for await r in a.zip(b)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.count == 3)
    }

    @Test func combineLatestEmitsOnEachUpdate() async {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<String, Never>()
        let values = Collector<String>()

        let cancellable = subject1.eraseToPublisher()
            .combineLatest(subject2.eraseToPublisher())
            .map { "\($0.0)\($0.1)" }
            .sink { values.append($0) }

        subject1.send(1)                                         // → latestA=1, no b yet
        try? await Task.sleep(nanoseconds: 10_000_000)           // let Task start and suspend on otherBox
        subject2.send("a")                                       // → latestB="a", emit (1,"a")
        try? await Task.sleep(nanoseconds: 10_000_000)
        subject1.send(2)                                         // → latestA=2, emit (2,"a")
        try? await Task.sleep(nanoseconds: 10_000_000)
        subject2.send("b")                                       // → latestB="b", emit (2,"b")
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()

        #expect(values.values == ["1a", "2a", "2b"])
    }
}

@Suite struct ErrorOperatorTests {
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
        try? await Task.sleep(nanoseconds: 20_000_000)
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
        try? await Task.sleep(nanoseconds: 20_000_000)
        cancellable.cancel()

        #expect(values.values == [1, 0])
    }

    @Test func retryRepeatsOnFailure() async {
        enum E: Error, Sendable { case boom }
        let counter = _AtomicCounter()

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
        try? await Task.sleep(nanoseconds: 50_000_000)
        cancellable.cancel()

        #expect(values.values == [1, 2, 3])
        #expect(completions.values == [.finished])
    }
}

@Suite struct ShareTests {
    @Test func shareMultiplexesToMultipleSubscribers() async {
        let subject = PassthroughSubject<Int, Never>()
        let shared = subject.eraseToPublisher().share()

        let c1 = Collector<Int>()
        let c2 = Collector<Int>()
        let cancel1 = shared.sink { c1.append($0) }
        let cancel2 = shared.sink { c2.append($0) }

        subject.send(1)
        subject.send(2)
        try? await Task.sleep(nanoseconds: 20_000_000)
        cancel1.cancel()
        cancel2.cancel()

        #expect(c1.values == [1, 2])
        #expect(c2.values == [1, 2])
    }

    @Test func connectablePublisherDeliversAfterConnect() async {
        let connectable = Publisher<Int, Never>.sequence(1...3).makeConnectable()
        let values = Collector<Int>()
        let sub = connectable.eraseToPublisher().sink { values.append($0) }
        let connection = connectable.connect()
        try? await Task.sleep(nanoseconds: 20_000_000)
        connection.cancel()
        sub.cancel()

        #expect(values.values == [1, 2, 3])
    }
}
