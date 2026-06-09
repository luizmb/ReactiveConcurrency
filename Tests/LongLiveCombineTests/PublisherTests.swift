import Foundation
import Testing
@testable import LongLiveCombine

// Thread-safe value collector for use in @Sendable sink closures.
final class Collector<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var _values: [T] = []
    var values: [T] { lock.withLock { _values } }
    func append(_ value: T) { lock.withLock { _values.append(value) } }
}

// Thread-safe monotonic counter for use in @Sendable closures.
final class _AtomicCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    var current: Int { lock.withLock { _value } }
    @discardableResult
    func increment() -> Int { lock.withLock { _value += 1; return _value } }
}

@Suite struct PublisherTests {
    @Test func justEmitsSingleValueThenFinishes() async {
        let values = Collector<Int>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let cancellable = Publisher<Int, Never>.just(42).sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { values.append($0) }
        )
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()

        #expect(values.values == [42])
        #expect(completions.values == [.finished])
    }

    @Test func emptyFinishesWithoutValues() async {
        let values = Collector<Int>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let cancellable = Publisher<Int, Never>.empty().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { values.append($0) }
        )
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()

        #expect(values.values.isEmpty)
        #expect(completions.values == [.finished])
    }

    @Test func sequenceEmitsAllElements() async {
        // Direct async iteration avoids @Sendable capture issues
        var collected: [Int] = []
        for await result in Publisher<Int, Never>.sequence(1...5)._stream {
            if case .success(let v) = result { collected.append(v) }
        }
        #expect(collected == [1, 2, 3, 4, 5])
    }

    @Test func mapTransformsValues() async {
        var collected: [String] = []
        for await result in Publisher<Int, Never>.sequence(1...3).map({ "\($0)" })._stream {
            if case .success(let v) = result { collected.append(v) }
        }
        #expect(collected == ["1", "2", "3"])
    }

    @Test func filterRemovesNonMatchingValues() async {
        var collected: [Int] = []
        for await result in Publisher<Int, Never>.sequence(1...6).filter({ $0.isMultiple(of: 2) })._stream {
            if case .success(let v) = result { collected.append(v) }
        }
        #expect(collected == [2, 4, 6])
    }

    @Test func cancellationStopsDelivery() async {
        let values = Collector<Int>()
        let subject = PassthroughSubject<Int, Never>()
        let cancellable = subject.eraseToPublisher().sink { values.append($0) }

        subject.send(1)
        subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()
        subject.send(3)
        try? await Task.sleep(nanoseconds: 10_000_000)

        #expect(values.values == [1, 2])
    }

    @Test func failPublisherDeliversFailure() async {
        enum E: Error, Equatable, Sendable { case boom }
        let completions = Collector<Subscribers.Completion<E>>()

        let cancellable = Publisher<Int, E>.fail(.boom).sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()

        #expect(completions.values == [.failure(.boom)])
    }

    @Test func continuationYieldAllSync() async {
        var collected: [Int] = []
        for await result in Publisher<Int, Never> { continuation in
            continuation.yieldAll(0..<5)
        }._stream {
            if case .success(let v) = result { collected.append(v) }
        }
        #expect(collected == [0, 1, 2, 3, 4])
    }

    @Test func currentValueSubjectReplaysCurrentValue() async {
        let subject = CurrentValueSubject<Int, Never>(10)
        let values = Collector<Int>()
        let cancellable = subject.eraseToPublisher().sink { values.append($0) }

        try? await Task.sleep(nanoseconds: 10_000_000)  // allow replay to arrive
        subject.send(20)
        try? await Task.sleep(nanoseconds: 10_000_000)
        cancellable.cancel()

        #expect(values.values == [10, 20])
    }
}

extension Subscribers.Completion: Equatable where Failure: Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.finished, .finished): true
        case (.failure(let l), .failure(let r)): l == r
        default: false
        }
    }
}
