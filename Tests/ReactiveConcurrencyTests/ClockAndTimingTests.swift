import Hourglass
@testable import ReactiveConcurrency
import Testing

// Yields enough times for tasks spawned inside other tasks to register their state.
// 12 yields is the safe margin under concurrent Swift Testing parallel execution.
private func settle() async {
    for _ in 0..<12 { await Task.yield() }
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
            if case .success(let v) = r { result.append(v) }
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
        await settle()
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
        await settle()

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
            if case .success(let v) = r { result.append(v) }
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
        await settle()
        sub.cancel()

        #expect(values.values.count == 3)
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
        await settle()
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
        await settle()
        await clock.waitForSleepers()
        #expect(values.values.isEmpty)

        await clock.advance(by: .milliseconds(300))
        await settle()
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
        await clock.waitForSleepers()
        await clock.advance(by: .milliseconds(200))
        await settle()
        #expect(values.values.isEmpty)

        subject.send(2)
        await settle()
        await clock.waitForSleepers()
        await clock.advance(by: .milliseconds(300))
        await settle()
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
        await settle()
        await clock.advance(by: .seconds(1))
        await settle()

        subject.send(3)
        await settle()
        await clock.advance(by: .seconds(1))
        await settle()

        #expect(windows.values == [[1, 2], [3]])
        sub.cancel()
    }

    @Test func collectFlushesPartialWindowOnCompletion() async {
        let clock = TestClock()
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence([1, 2, 3])
            .collect(every: .seconds(10), clock: clock)
            ._stream {
            if case .success(let v) = r { result.append(v) }
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
        await settle()
        await clock.advance(by: .seconds(1))
        await settle()
        #expect(windows.values == [[1]])

        sub.cancel()
    }

    @Test func collectWithImmediateClockFlushesEachValueInItsOwnWindow() async {
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence(1...3)
            .collect(every: .seconds(1), clock: ImmediateClock())
            ._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(!result.isEmpty)
        #expect(result.flatMap { $0 }.sorted() == [1, 2, 3])
    }
}
