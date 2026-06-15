import Foundation
@testable import ReactiveConcurrency
import Testing

// Lets pending Tasks (upstream subscription registration, pump start-up) run before we
// send into a hot subject. Yields generously to stay deterministic under parallel CI load.
private func settle() async {
    for _ in 0..<20 { await Task.yield() }
}

// Polls a condition instead of sleeping a fixed amount: the upstream→core pump is async and
// can be scheduled late on slower runners (e.g. Linux CI), so a fixed sleep is flaky.
private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    for _ in 0..<(timeoutMs / 2) {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

@Suite struct ShareTests {
    @Test func shareDeliversToBothSubscribers() async {
        let subject = PassthroughSubject<Int, Never>()
        let shared = subject.eraseToPublisher().share()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = shared.sink { v1.append($0) }
        let c2 = shared.sink { v2.append($0) }
        await settle()
        subject.send(1); subject.send(2)
        await poll { v1.values.count >= 2 && v2.values.count >= 2 }
        c1.cancel(); c2.cancel()

        #expect(v1.values == [1, 2])
        #expect(v2.values == [1, 2])
    }

    @Test func shareRunsUpstreamOnce() async {
        let subscriptionCount = AtomicCounter()
        let subject = PassthroughSubject<Int, Never>()

        let upstream = Publisher<Int, Never> { continuation in
            subscriptionCount.increment()
            await continuation.suspendUntilCancelled()
        }
        let shared = upstream.share()

        let c1 = shared.sink { _ in }
        let c2 = shared.sink { _ in }
        await poll { subscriptionCount.current >= 1 }

        #expect(subscriptionCount.current == 1)
        c1.cancel(); c2.cancel()
    }

    @Test func cancellingOneSubscriberDoesNotAffectOthers() async {
        let subject = PassthroughSubject<Int, Never>()
        let shared = subject.eraseToPublisher().share()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = shared.sink { v1.append($0) }
        let c2 = shared.sink { v2.append($0) }
        await settle()
        subject.send(1)
        await poll { v1.values.count >= 1 && v2.values.count >= 1 }
        c1.cancel()
        subject.send(2)
        await poll { v2.values.count >= 2 }
        c2.cancel()

        #expect(v1.values == [1])
        #expect(v2.values == [1, 2])
    }

    @Test func shareCompletionDeliveredToAllSubscribers() async {
        let subject = PassthroughSubject<Int, Never>()
        let shared = subject.eraseToPublisher().share()
        let c1 = Collector<Subscribers.Completion<Never>>()
        let c2 = Collector<Subscribers.Completion<Never>>()

        let s1 = shared.sink(receiveCompletion: { c1.append($0) }, receiveValue: { _ in })
        let s2 = shared.sink(receiveCompletion: { c2.append($0) }, receiveValue: { _ in })
        await settle()
        subject.send(completion: .finished)
        await poll { !c1.values.isEmpty && !c2.values.isEmpty }
        s1.cancel(); s2.cancel()

        #expect(c1.values == [.finished])
        #expect(c2.values == [.finished])
    }

    @Test func shareRestartsUpstreamAfterAllSubscribersCancel() async {
        let subscriptionCount = AtomicCounter()
        let subject = PassthroughSubject<Int, Never>()

        let upstream = Publisher<Int, Never> { continuation in
            subscriptionCount.increment()
            await continuation.suspendUntilCancelled()
        }
        let shared = upstream.share()

        let c1 = shared.sink { _ in }
        await poll { subscriptionCount.current >= 1 }
        c1.cancel()
        await settle()

        let c2 = shared.sink { _ in }
        await poll { subscriptionCount.current >= 2 }
        c2.cancel()

        #expect(subscriptionCount.current == 2)
        _ = subject
    }
}

@Suite struct ConnectablePublisherTests {
    @Test func subscribersReceiveValuesAfterConnect() async {
        let subject = PassthroughSubject<Int, Never>()
        let connectable = subject.eraseToPublisher().makeConnectable()
        let values = Collector<Int>()

        let c = connectable.eraseToPublisher().sink { values.append($0) }
        let connection = connectable.connect()
        await settle()
        subject.send(1); subject.send(2)
        await poll { values.values.count >= 2 }
        c.cancel(); connection.cancel()

        #expect(values.values == [1, 2])
    }

    @Test func cancellingConnectionStopsUpstream() async {
        let subject = PassthroughSubject<Int, Never>()
        let connectable = subject.eraseToPublisher().makeConnectable()
        let values = Collector<Int>()

        let c = connectable.eraseToPublisher().sink { values.append($0) }
        let connection = connectable.connect()
        await settle()
        subject.send(1)
        await poll { values.values.count >= 1 }
        connection.cancel()
        subject.send(2)
        await settle()
        c.cancel()

        #expect(values.values == [1])
    }

    @Test func multipleSubscribersOnConnectableAllReceiveValues() async {
        let subject = PassthroughSubject<Int, Never>()
        let connectable = subject.eraseToPublisher().makeConnectable()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = connectable.eraseToPublisher().sink { v1.append($0) }
        let c2 = connectable.eraseToPublisher().sink { v2.append($0) }
        let connection = connectable.connect()
        await settle()
        subject.send(1); subject.send(2)
        await poll { v1.values.count >= 2 && v2.values.count >= 2 }
        c1.cancel(); c2.cancel(); connection.cancel()

        #expect(v1.values == [1, 2])
        #expect(v2.values == [1, 2])
    }

    @Test func autoconnectStartsUpstreamOnFirstSubscription() async {
        let subject = PassthroughSubject<Int, Never>()
        let pub = subject.eraseToPublisher().makeConnectable().autoconnect()
        let values = Collector<Int>()

        let c = pub.sink { values.append($0) }
        await settle()
        subject.send(1); subject.send(2)
        await poll { values.values.count >= 2 }
        c.cancel()

        #expect(values.values == [1, 2])
    }

    @Test func autoconnectMultipleSubscribersShareUpstream() async {
        let subscriptionCount = AtomicCounter()
        let subject = PassthroughSubject<Int, Never>()

        let upstream = Publisher<Int, Never> { continuation in
            subscriptionCount.increment()
            await continuation.suspendUntilCancelled()
        }
        let pub = upstream.makeConnectable().autoconnect()

        let c1 = pub.sink { _ in }
        let c2 = pub.sink { _ in }
        await poll { subscriptionCount.current >= 1 }

        #expect(subscriptionCount.current == 1)
        c1.cancel(); c2.cancel()
        _ = subject
    }
}
