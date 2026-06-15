import Foundation
import Testing
@testable import ReactiveConcurrency

@Suite struct ShareTests {
    @Test func shareDeliversToBothSubscribers() async {
        let subject = PassthroughSubject<Int, Never>()
        let shared = subject.eraseToPublisher().share()
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = shared.sink { v1.append($0) }
        let c2 = shared.sink { v2.append($0) }
        subject.send(1); subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        try? await Task.sleep(nanoseconds: 10_000_000)

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
        subject.send(1)
        try? await Task.sleep(nanoseconds: 10_000_000)
        c1.cancel()
        subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        subject.send(completion: .finished)
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        try? await Task.sleep(nanoseconds: 10_000_000)
        c1.cancel()
        try? await Task.sleep(nanoseconds: 10_000_000)

        let c2 = shared.sink { _ in }
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        subject.send(1); subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
        c.cancel(); connection.cancel()

        #expect(values.values == [1, 2])
    }

    @Test func cancellingConnectionStopsUpstream() async {
        let subject = PassthroughSubject<Int, Never>()
        let connectable = subject.eraseToPublisher().makeConnectable()
        let values = Collector<Int>()

        let c = connectable.eraseToPublisher().sink { values.append($0) }
        let connection = connectable.connect()
        subject.send(1)
        try? await Task.sleep(nanoseconds: 10_000_000)
        connection.cancel()
        subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        subject.send(1); subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
        c1.cancel(); c2.cancel(); connection.cancel()

        #expect(v1.values == [1, 2])
        #expect(v2.values == [1, 2])
    }

    @Test func autoconnectStartsUpstreamOnFirstSubscription() async {
        let subject = PassthroughSubject<Int, Never>()
        let pub = subject.eraseToPublisher().makeConnectable().autoconnect()
        let values = Collector<Int>()

        let c = pub.sink { values.append($0) }
        subject.send(1); subject.send(2)
        try? await Task.sleep(nanoseconds: 10_000_000)
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
        try? await Task.sleep(nanoseconds: 10_000_000)
        c1.cancel(); c2.cancel()

        #expect(subscriptionCount.current == 1)
        _ = subject
    }
}
