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

@Suite struct PassthroughSubjectTests {
    @Test func sendDeliversValueToSubscriber() async {
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()

        let c = subject.eraseToPublisher().sink { values.append($0) }
        subject.send(1); subject.send(2)
        await poll(until: { values.values.count == 2 })
        c.cancel()

        #expect(values.values == [1, 2])
    }

    @Test func lateSubscriberMissesValues() async {
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()

        subject.send(1)
        let c = subject.eraseToPublisher().sink { values.append($0) }
        subject.send(2)
        await poll(until: { values.values.count == 1 })
        c.cancel()

        #expect(values.values == [2])
    }

    @Test func multipleSubscribersAllReceiveValues() async {
        let subject = PassthroughSubject<Int, Never>()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = subject.eraseToPublisher().sink { v1.append($0) }
        let c2 = subject.eraseToPublisher().sink { v2.append($0) }
        subject.send(1); subject.send(2)
        await poll(until: { v1.values.count == 2 && v2.values.count == 2 })
        c1.cancel(); c2.cancel()

        #expect(v1.values == [1, 2])
        #expect(v2.values == [1, 2])
    }

    @Test func cancelledSubscriberStopsReceiving() async {
        let subject = PassthroughSubject<Int, Never>()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = subject.eraseToPublisher().sink { v1.append($0) }
        let c2 = subject.eraseToPublisher().sink { v2.append($0) }
        subject.send(1)
        await poll(until: { v1.values.count == 1 && v2.values.count == 1 })
        c1.cancel()
        subject.send(2)
        await poll(until: { v2.values.count == 2 })
        c2.cancel()

        #expect(v1.values == [1])
        #expect(v2.values == [1, 2])
    }

    @Test func finishedCompletionDelivered() async {
        let subject = PassthroughSubject<Int, Never>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let c = subject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        subject.send(completion: .finished)
        await poll(until: { completions.values.count == 1 })
        c.cancel()

        #expect(completions.values == [.finished])
    }

    @Test func failureCompletionDelivered() async {
        enum E: Error, Equatable, Sendable { case boom }
        let subject = PassthroughSubject<Int, E>()
        let completions = Collector<Subscribers.Completion<E>>()

        let c = subject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        subject.send(completion: .failure(.boom))
        await poll(until: { completions.values.count == 1 })
        c.cancel()

        #expect(completions.values == [.failure(.boom)])
    }

    @Test func dropsValuesAfterCompletion() async {
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()

        let c = subject.eraseToPublisher().sink { values.append($0) }
        subject.send(1)
        subject.send(completion: .finished)
        subject.send(2)
        await poll(until: { values.values.count == 1 })
        c.cancel()

        #expect(values.values == [1])
    }

    @Test func alreadyCompletedSubjectImmediatelyCompletesLateSubscriber() async {
        let subject = PassthroughSubject<Int, Never>()
        let completions = Collector<Subscribers.Completion<Never>>()

        subject.send(completion: .finished)

        let c = subject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        await poll(until: { completions.values.count == 1 })
        c.cancel()

        #expect(completions.values == [.finished])
    }
}

@Suite struct CurrentValueSubjectTests {
    @Test func replayInitialValueOnSubscription() async {
        let subject = CurrentValueSubject<Int, Never>(42)
        let values = Collector<Int>()

        let c = subject.eraseToPublisher().sink { values.append($0) }
        await poll(until: { values.values.count == 1 })
        c.cancel()

        #expect(values.values == [42])
    }

    @Test func valuePropertyReflectsLatestSend() async {
        let subject = CurrentValueSubject<Int, Never>(1)
        #expect(subject.value == 1)
        subject.send(2)
        #expect(subject.value == 2)
        subject.send(3)
        #expect(subject.value == 3)
    }

    @Test func deliversNewValuesToSubscriber() async {
        let subject = CurrentValueSubject<Int, Never>(0)
        let values = Collector<Int>()

        let c = subject.eraseToPublisher().sink { values.append($0) }
        subject.send(1); subject.send(2)
        await poll(until: { values.values.count == 3 })
        c.cancel()

        #expect(values.values == [0, 1, 2])
    }

    @Test func lateSubscriberGetsCurrentValue() async {
        let subject = CurrentValueSubject<Int, Never>(0)
        subject.send(1); subject.send(2)

        let values = Collector<Int>()
        let c = subject.eraseToPublisher().sink { values.append($0) }
        await poll(until: { values.values.count == 1 })
        c.cancel()

        #expect(values.values == [2])
    }

    @Test func finishedCompletionDelivered() async {
        let subject = CurrentValueSubject<Int, Never>(0)
        let completions = Collector<Subscribers.Completion<Never>>()

        let c = subject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { _ in }
        )
        subject.send(completion: .finished)
        await poll(until: { completions.values.count == 1 })
        c.cancel()

        #expect(completions.values == [.finished])
    }
}

@Suite struct AnySubjectTests {
    @Test func eraseToAnySubjectForwardsSendAndCompletion() async {
        let subject = PassthroughSubject<Int, Never>()
        let anySubject = subject.eraseToAnySubject()
        let values = Collector<Int>()
        let completions = Collector<Subscribers.Completion<Never>>()

        let c = anySubject.eraseToPublisher().sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { values.append($0) }
        )
        anySubject.send(1); anySubject.send(2)
        anySubject.send(completion: .finished)
        await poll(until: { values.values.count == 2 && completions.values.count == 1 })
        c.cancel()

        #expect(values.values == [1, 2])
        #expect(completions.values == [.finished])
    }
}
