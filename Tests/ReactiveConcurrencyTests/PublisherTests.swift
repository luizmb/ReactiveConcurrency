// SPDX-License-Identifier: Apache-2.0

import Foundation
@testable import ReactiveConcurrency
import Testing

// Polls a condition instead of a fixed sleep — values/completions arrive on a consumer Task that
// can be scheduled late under parallel execution on a constrained CPU, making fixed sleeps flaky.
private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    for _ in 0..<(timeoutMs / 2) {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

// Yields repeatedly so already-scheduled consumer Tasks drain, when we expect NO further delivery
// (e.g. after cancellation) and therefore have no count to poll for.
private func settle() async {
    for _ in 0..<20 {
        await Task.yield()
    }
}

// Thread-safe value collector for use in @Sendable sink closures.
final class Collector<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var _values: [T] = []
    var values: [T] { lock.withLock { _values } }
    func append(_ value: T) { lock.withLock { _values.append(value) } }
}

// Thread-safe monotonic counter for use in @Sendable closures.
final class AtomicCounter: @unchecked Sendable {
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
        await poll(until: { values.values.count == 1 && completions.values.count == 1 })
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
        await poll(until: { completions.values.count == 1 })
        cancellable.cancel()

        #expect(values.values.isEmpty)
        #expect(completions.values == [.finished])
    }

    @Test func sequenceEmitsAllElements() async {
        // Direct async iteration avoids @Sendable capture issues
        var collected: [Int] = []
        for await result in Publisher<Int, Never>.sequence(1...5)._stream {
            if case let .success(v) = result { collected.append(v) }
        }
        #expect(collected == [1, 2, 3, 4, 5])
    }

    @Test func mapTransformsValues() async {
        var collected: [String] = []
        for await result in Publisher<Int, Never>.sequence(1...3).map({ "\($0)" })._stream {
            if case let .success(v) = result { collected.append(v) }
        }
        #expect(collected == ["1", "2", "3"])
    }

    @Test func filterRemovesNonMatchingValues() async {
        var collected: [Int] = []
        for await result in Publisher<Int, Never>.sequence(1...6).filter({ $0.isMultiple(of: 2) })._stream {
            if case let .success(v) = result { collected.append(v) }
        }
        #expect(collected == [2, 4, 6])
    }

    @Test func cancellationStopsDelivery() async {
        let values = Collector<Int>()
        let subject = PassthroughSubject<Int, Never>()
        let cancellable = subject.eraseToPublisher().sink { values.append($0) }

        subject.send(1)
        subject.send(2)
        await poll(until: { values.values.count == 2 })
        cancellable.cancel()
        subject.send(3)
        await settle()

        #expect(values.values == [1, 2])
    }

    // Cancellation must NOT invoke the completion handler — Combine's contract.
    @Test func cancellationDoesNotCallCompletion() async {
        enum E: Error, Equatable, Sendable { case boom }
        let completions = Collector<Subscribers.Completion<E>>()
        let subject = PassthroughSubject<Int, E>()

        let cancellable = subject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        subject.send(1)
        await settle()
        cancellable.cancel()
        await settle()

        #expect(completions.values.isEmpty)
    }

    // Dropping the AnyCancellable token tears down the subscription.
    // The guard !Task.isCancelled check in the loop prevents buffered values from
    // being delivered to a pre-cancelled Task (cancelled before it ran its first iteration).
    @Test func droppedCancellableStopsDelivery() async {
        let values = Collector<Int>()
        let subject = PassthroughSubject<Int, Never>()

        do {
            let cancellable = subject.eraseToPublisher().sink { values.append($0) }
            _ = cancellable // keep alive until here
        }
        // deinit fires, task.cancel() called before the Task has run

        subject.send(1)
        await settle()

        #expect(values.values.isEmpty)
    }

    @Test func failPublisherDeliversFailure() async {
        enum E: Error, Equatable, Sendable { case boom }
        let completions = Collector<Subscribers.Completion<E>>()

        let cancellable = Publisher<Int, E>.fail(.boom).sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        await poll(until: { completions.values.count == 1 })
        cancellable.cancel()

        #expect(completions.values == [.failure(.boom)])
    }

    @Test func continuationYieldAllSync() async {
        var collected: [Int] = []
        for await result in Publisher<Int, Never> { continuation in
            continuation.yieldAll(0..<5)
        }._stream {
            if case let .success(v) = result { collected.append(v) }
        }
        #expect(collected == [0, 1, 2, 3, 4])
    }

    @Test func currentValueSubjectReplaysCurrentValue() async {
        let subject = CurrentValueSubject<Int, Never>(10)
        let values = Collector<Int>()
        let cancellable = subject.eraseToPublisher().sink { values.append($0) }

        subject.send(20)
        await poll(until: { values.values.count == 2 })
        cancellable.cancel()

        #expect(values.values == [10, 20])
    }
}

extension Subscribers.Completion: Equatable where Failure: Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.finished, .finished): true
        case let (.failure(l), .failure(r)): l == r
        default: false
        }
    }
}
