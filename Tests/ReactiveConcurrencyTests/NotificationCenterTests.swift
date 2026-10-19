// SPDX-License-Identifier: Apache-2.0

import Foundation
@testable import ReactiveConcurrency
import Testing

private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    let deadline = ContinuousClock.now + .milliseconds(timeoutMs)
    while !condition(), ContinuousClock.now < deadline {
        try? await Task.sleep(for: .milliseconds(5))
    }
}

private let ping = Notification.Name("ReactiveConcurrencyTests.ping")

/// Posts `ping` numbered `0` until the subscription has seen one, so the numbered posts that
/// follow cannot race the observer's registration.
private func primed(_ center: NotificationCenter, _ values: Collector<Int>) async {
    await poll {
        center.post(name: ping, object: nil, userInfo: ["n": 0])
        return !values.values.isEmpty
    }
}

private func number(_ notification: Notification) -> Int {
    notification.userInfo?["n"] as? Int ?? -1
}

/// Counts the observers taken off it, to see a subscription's end remove its own.
private final class CountingCenter: NotificationCenter, @unchecked Sendable {
    let removed = AtomicCounter()
    override func removeObserver(_ observer: Any) {
        removed.increment()
        super.removeObserver(observer)
    }
}

private final class Sender: Sendable {}

@Suite(.timeLimit(.minutes(1))) struct NotificationCenterTests {
    @Test func publishesWhatTheTransformReadsOffEachPost() async {
        let center = NotificationCenter()
        let values = Collector<Int>()
        let sub = center.notifications(named: ping, number).sink { values.append($0) }
        await primed(center, values)

        center.post(name: ping, object: nil, userInfo: ["n": 1])
        center.post(name: ping, object: nil, userInfo: ["n": 2])
        await poll { values.values.last == 2 }

        #expect(Array(values.values.drop { $0 == 0 }) == [1, 2])
        sub.cancel()
    }

    @Test func onlyTheGivenObjectsPostsWhenOneIsGiven() async {
        let center = NotificationCenter()
        let mine = Sender()
        let values = Collector<Int>()
        let sub = center.notifications(named: ping, object: mine, number).sink { values.append($0) }
        await poll {
            center.post(name: ping, object: mine, userInfo: ["n": 0])
            return !values.values.isEmpty
        }

        center.post(name: ping, object: Sender(), userInfo: ["n": 1])
        center.post(name: ping, object: mine, userInfo: ["n": 2])
        await poll { values.values.last == 2 }

        #expect(Array(values.values.drop { $0 == 0 }) == [2])
        sub.cancel()
    }

    @Test func theBareFactCarriesNoPayload() async {
        let center = NotificationCenter()
        let count = AtomicCounter()
        let sub = center.notifications(named: ping).sink { count.increment() }
        await poll {
            center.post(name: ping, object: nil)
            return count.current > 0
        }
        #expect(count.current > 0)
        sub.cancel()
    }

    @Test func eachSubscriptionIsItsOwnObserver() async {
        let center = NotificationCenter()
        let first = Collector<Int>()
        let second = Collector<Int>()
        let source = center.notifications(named: ping, number)
        let subs = [source.sink { first.append($0) }, source.sink { second.append($0) }]
        await poll {
            center.post(name: ping, object: nil, userInfo: ["n": 0])
            return !first.values.isEmpty && !second.values.isEmpty
        }

        center.post(name: ping, object: nil, userInfo: ["n": 7])
        await poll { first.values.last == 7 && second.values.last == 7 }

        #expect(first.values.last == 7)
        #expect(second.values.last == 7)
        subs.forEach { $0.cancel() }
    }

    @Test func endingTheSubscriptionRemovesItsObserver() async {
        let center = CountingCenter()
        let values = Collector<Int>()
        let sub = center.notifications(named: ping, number).sink { values.append($0) }
        await primed(center, values)
        #expect(center.removed.current == 0)

        sub.cancel()
        await poll { center.removed.current == 1 }

        #expect(center.removed.current == 1)
    }
}
