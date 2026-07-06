// SPDX-License-Identifier: Apache-2.0

import Hourglass
@testable import ReactiveConcurrency
import Testing

// Yields enough times for tasks spawned inside other tasks to register their state.
// 12 yields is the safe margin under concurrent Swift Testing parallel execution.
private func settle() async {
    for _ in 0..<12 {
        await Task.yield()
    }
}

// Polls a condition rather than relying on a fixed number of yields: after a TestClock
// advance, the timer-driven flush is processed on a separate Task that can be scheduled
// late on slower runners (e.g. Linux CI).
private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    for _ in 0..<(timeoutMs / 2) {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

// Consumption barrier for collect(every:): its flush timer sleeps independently of value
// consumption, so waitForSleepers() does NOT gate it (unlike delay/debounce). Used only
// while the clock has not advanced — no tick can fire yet — so this purely waits for already
// sent values to be drained into the operator's bucket before we advance to trigger a flush.
private func drainSentValues() async {
    for _ in 0..<60 {
        await Task.yield()
        try? await Task.sleep(nanoseconds: 1_000_000)
    }
}

// MARK: - ImmediateClock

@Suite struct ImmediateClockTests {
    @Test func sleepReturnsImmediately() async {
        let clock = ImmediateClock()
        let start = clock.now
        try? await clock.sleep(until: start.advanced(by: .seconds(60)), tolerance: nil)
        // If this test completes, sleep was indeed immediate
    }

    @Test func delayWithImmediateClockPassesThrough() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...3)
            .delay(for: .seconds(10), clock: ImmediateClock())
            ._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }
}

// MARK: - TestClock

@Suite struct TestClockTests {
    @Test func initialNowIsZero() {
        let clock = TestClock()
        #expect(clock.now.offset == .zero)
    }

    @Test func advanceMovesNow() async {
        let clock = TestClock()
        await clock.advance(by: .seconds(5))
        #expect(clock.now.offset == .seconds(5))
    }

    @Test func sleepSuspendsUntilAdvanced() async {
        let clock = TestClock()
        let awoke = AtomicCounter()

        let task = Task {
            try? await clock.sleep(until: clock.now.advanced(by: .seconds(1)), tolerance: nil)
            awoke.increment()
        }

        await clock.waitForSleepers()
        #expect(awoke.current == 0)

        await clock.advance(by: .seconds(1))
        await poll { awoke.current >= 1 }
        #expect(awoke.current == 1)
        task.cancel()
    }

    @Test func advanceWakesMultipleSleepersInOrder() async {
        let clock = TestClock()
        let order = Collector<Int>()

        let t1 = Task {
            try? await clock.sleep(until: clock.now.advanced(by: .seconds(1)), tolerance: nil)
            order.append(1)
        }
        let t2 = Task {
            try? await clock.sleep(until: clock.now.advanced(by: .seconds(2)), tolerance: nil)
            order.append(2)
        }

        await clock.waitForSleepers(count: 2)
        await clock.advance(by: .seconds(2))
        await poll { order.values.count >= 2 }

        #expect(order.values == [1, 2])
        t1.cancel(); t2.cancel()
    }
}

// MARK: - Timer publisher

@Suite struct TimerPublisherTests {
    @Test func timerEmitsWithImmediateClock() async {
        var result: [ImmediateClock.Instant] = []
        let stream = Publisher<ImmediateClock.Instant, Never>
            .timer(every: .seconds(1), clock: ImmediateClock())
            .prefix(3)._stream
        for await r in stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result.count == 3)
    }

    @Test func timerEmitsWithTestClock() async {
        let clock = TestClock()
        let values = Collector<TestClock.Instant>()
        let sub = Publisher<TestClock.Instant, Never>
            .timer(every: .seconds(1), clock: clock)
            .sink { values.append($0) }

        await clock.waitForSleepers()
        await clock.advance(by: .seconds(1))
        await clock.waitForSleepers()
        await clock.advance(by: .seconds(1))
        await clock.waitForSleepers()
        await clock.advance(by: .seconds(1))
        await poll { values.values.count >= 3 }
        #expect(values.values.count == 3)
        sub.cancel()
    }
}

// MARK: - delay with TestClock

@Suite struct DelayWithTestClockTests {
    @Test func delayHoldsValuesUntilClockAdvances() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()
        let sub = subject.eraseToPublisher()
            .delay(for: .seconds(1), clock: clock)
            .sink { values.append($0) }

        await settle()
        subject.send(1)
        await clock.waitForSleepers()
        #expect(values.values.isEmpty)

        await clock.advance(by: .seconds(1))
        await poll { values.values.count >= 1 }
        #expect(values.values == [1])

        sub.cancel()
    }
}

// MARK: - debounce with TestClock

@Suite struct DebounceWithTestClockTests {
    @Test func debounceEmitsLastValueAfterQuietPeriod() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()
        let sub = subject.eraseToPublisher()
            .debounce(for: .milliseconds(300), clock: clock)
            .sink { values.append($0) }

        await settle()
        subject.send(1); subject.send(2); subject.send(3)
        await drainSentValues()
        await clock.waitForSleepers()
        #expect(values.values.isEmpty)

        await clock.advance(by: .milliseconds(300))
        await poll { values.values.count >= 1 }
        #expect(values.values == [3])

        sub.cancel()
    }

    @Test func debounceResetsTimerOnNewValue() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let values = Collector<Int>()
        let sub = subject.eraseToPublisher()
            .debounce(for: .milliseconds(300), clock: clock)
            .sink { values.append($0) }

        await settle()
        subject.send(1)
        await drainSentValues()
        await clock.advance(by: .milliseconds(200))
        await settle()
        #expect(values.values.isEmpty)

        subject.send(2)
        await drainSentValues()
        await clock.advance(by: .milliseconds(300))
        await poll { values.values.count >= 1 }
        #expect(values.values == [2])

        sub.cancel()
    }
}

// MARK: - collect(every:clock:)

@Suite struct CollectByTimeTests {
    @Test func collectGroupsValuesIntoWindows() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let windows = Collector<[Int]>()
        let sub = subject.eraseToPublisher()
            .collect(every: .seconds(1), clock: clock)
            .sink { windows.append($0) }

        await settle()
        subject.send(1); subject.send(2)
        await drainSentValues()
        await clock.advance(by: .seconds(1))
        await poll { windows.values.count >= 1 }

        subject.send(3)
        await drainSentValues()
        await clock.advance(by: .seconds(1))
        await poll { windows.values.count >= 2 }

        #expect(windows.values == [[1, 2], [3]])
        sub.cancel()
    }

    @Test func collectFlushesPartialWindowOnCompletion() async {
        let clock = TestClock()
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence([1, 2, 3])
            .collect(every: .seconds(10), clock: clock)
            ._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(result == [[1, 2, 3]])
    }

    @Test func collectEmptyWindowsAreSkipped() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let windows = Collector<[Int]>()
        let sub = subject.eraseToPublisher()
            .collect(every: .seconds(1), clock: clock)
            .sink { windows.append($0) }

        await clock.waitForSleepers()
        await clock.advance(by: .seconds(1))
        await settle()
        #expect(windows.values.isEmpty)

        subject.send(1)
        await drainSentValues()
        await clock.advance(by: .seconds(1))
        await poll { windows.values.count >= 1 }
        #expect(windows.values == [[1]])

        sub.cancel()
    }

    @Test func collectWithImmediateClockFlushesEachValueInItsOwnWindow() async {
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence(1...3)
            .collect(every: .seconds(1), clock: ImmediateClock())
            ._stream {
            if case let .success(v) = r { result.append(v) }
        }
        #expect(!result.isEmpty)
        #expect(result.flatMap { $0 }.sorted() == [1, 2, 3])
    }
}

// MARK: - timeout

private enum TimeoutTestError: Error, Equatable { case timedOut, upstreamBoom }

@Suite struct TimeoutOperatorTests {
    @Test func timeoutFiresWhenNoValueArrives() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, TimeoutTestError>()
        let values = Collector<Int>()
        let failures = Collector<TimeoutTestError>()

        let sub = subject.eraseToPublisher()
            .timeout(.seconds(1), clock: clock, error: .timedOut)
            .sink(
                receiveCompletion: { if case let .failure(e) = $0 { failures.append(e) } },
                receiveValue: { values.append($0) }
            )

        await settle()
        await clock.waitForSleepers() // timeout's internal deadline armed
        await clock.advance(by: .seconds(1)) // no value arrived → fire
        await poll { !failures.values.isEmpty }
        #expect(values.values.isEmpty)
        #expect(failures.values == [.timedOut])

        sub.cancel()
    }

    @Test func timeoutForwardsValuesThenFiresAfterSilence() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, TimeoutTestError>()
        let values = Collector<Int>()
        let failures = Collector<TimeoutTestError>()

        let sub = subject.eraseToPublisher()
            .timeout(.seconds(1), clock: clock, error: .timedOut)
            .sink(
                receiveCompletion: { if case let .failure(e) = $0 { failures.append(e) } },
                receiveValue: { values.append($0) }
            )

        await settle()
        subject.send(1)
        await poll { values.values.count >= 1 } // forwarded; deadline re-armed
        #expect(failures.values.isEmpty)

        await clock.waitForSleepers()
        await clock.advance(by: .seconds(1)) // now silent past the window → fire
        await poll { !failures.values.isEmpty }
        #expect(values.values == [1])
        #expect(failures.values == [.timedOut])

        sub.cancel()
    }

    @Test func timeoutDoesNotFireWhenUpstreamCompletesInTime() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, TimeoutTestError>()
        let values = Collector<Int>()
        let failures = Collector<TimeoutTestError>()
        let finished = AtomicCounter()

        let sub = subject.eraseToPublisher()
            .timeout(.seconds(1), clock: clock, error: .timedOut)
            .sink(
                receiveCompletion: {
                    switch $0 {
                    case .finished: finished.increment()
                    case let .failure(e): failures.append(e)
                    }
                },
                receiveValue: { values.append($0) }
            )

        await settle()
        subject.send(1)
        await poll { values.values.count >= 1 }
        subject.send(completion: .finished)
        await poll { finished.current >= 1 }
        #expect(values.values == [1])
        #expect(failures.values.isEmpty)
        #expect(finished.current == 1)

        sub.cancel()
    }

    @Test func upstreamFailurePreemptsTimeout() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, TimeoutTestError>()
        let failures = Collector<TimeoutTestError>()

        let sub = subject.eraseToPublisher()
            .timeout(.seconds(1), clock: clock, error: .timedOut)
            .sink(
                receiveCompletion: { if case let .failure(e) = $0 { failures.append(e) } },
                receiveValue: { _ in }
            )

        await settle()
        subject.send(completion: .failure(.upstreamBoom))
        await poll { !failures.values.isEmpty }
        #expect(failures.values == [.upstreamBoom]) // upstream error, not the timeout error

        sub.cancel()
    }
}

// MARK: - collect(every:orCount:)

@Suite struct CollectByTimeOrCountPublisherTests {
    @Test func flushesByCountBeforeTime() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let windows = Collector<[Int]>()
        let sub = subject.eraseToPublisher()
            .collect(every: .seconds(10), orCount: 3, clock: clock)
            .sink { windows.append($0) }

        await settle()
        subject.send(1); subject.send(2); subject.send(3) // count reached — flush without advancing
        await poll { windows.values.count >= 1 }
        #expect(windows.values == [[1, 2, 3]])
        sub.cancel()
    }

    @Test func flushesByTimeWhenCountNotReached() async {
        let clock = TestClock()
        let subject = PassthroughSubject<Int, Never>()
        let windows = Collector<[Int]>()
        let sub = subject.eraseToPublisher()
            .collect(every: .seconds(1), orCount: 5, clock: clock)
            .sink { windows.append($0) }

        await settle()
        subject.send(1); subject.send(2)
        await drainSentValues()
        await clock.advance(by: .seconds(1))
        await poll { windows.values.count >= 1 }
        #expect(windows.values == [[1, 2]])
        sub.cancel()
    }
}
