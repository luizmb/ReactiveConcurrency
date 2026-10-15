// SPDX-License-Identifier: Apache-2.0

// Operators that feed one downstream from two tasks (a producer reading upstream and a consumer
// running a timed transform) must deliver an upstream failure exactly once, as the last event.
// Each test runs the scenario many times concurrently: the race only shows under contention.

import Hourglass
@testable import ReactiveConcurrency
import Testing

private enum OrderingError: Error, Equatable { case boom, timedOut }

private func results<A: Sendable>(_ publisher: Publisher<A, OrderingError>) async -> [Result<A, OrderingError>] {
    var out: [Result<A, OrderingError>] = []
    for await result in publisher._stream {
        out.append(result)
    }
    return out
}

// Enough runs to catch the rarest race (about 1 in 2 000 before the fix) on a desktop CPU. The
// Android CI emulator is far slower, so it runs a smoke-sized batch.
#if os(Android)
    private let defaultRuns = 500
#else
    private let defaultRuns = 20_000
#endif

private func lostFailures<A: Sendable>(
    runs: Int = defaultRuns,
    _ make: @escaping @Sendable () -> Publisher<A, OrderingError>
) async -> Int {
    await withTaskGroup(of: Bool.self) { group in
        for _ in 0..<runs {
            group.addTask {
                // The failure must arrive, exactly once, as the last event.
                let events = await results(make())
                guard case .failure(.boom)? = events.last else { return true }
                return events.filter { if case .failure = $0 { true } else { false } }.count != 1
            }
        }
        var lost = 0
        for await failed in group where failed {
            lost += 1
        }
        return lost
    }
}

@Suite(.timeLimit(.minutes(1))) struct TerminalOrderingTests {
    @Test func timeoutNeverLosesUpstreamFailure() async {
        let lost = await lostFailures {
            Publisher<Int, OrderingError>.fail(.boom).timeout(.seconds(1), clock: TestClock(), error: .timedOut)
        }
        #expect(lost == 0)
    }

    @Test func timedOperatorNeverLosesUpstreamFailure() async {
        let lost = await lostFailures {
            Publisher<Int, OrderingError>.fail(.boom).collect(every: .seconds(1), clock: TestClock())
        }
        #expect(lost == 0)
    }

    @Test func mergeEndsWithExactlyOneFailure() async {
        let lost = await lostFailures {
            values(40).merge(with: .fail(.boom))
        }
        #expect(lost == 0)
    }

    @Test func mergeManyEndsWithExactlyOneFailure() async {
        let lost = await lostFailures {
            .merge([values(20), .fail(.boom), values(20)])
        }
        #expect(lost == 0)
    }

    @Test func flatMapEndsWithExactlyOneFailure() async {
        let lost = await lostFailures {
            values(3).flatMap { n in n == 1 ? .fail(.boom) : values(20) }
        }
        #expect(lost == 0)
    }

    @Test func boundedFlatMapEndsWithExactlyOneFailure() async {
        let lost = await lostFailures {
            values(3).flatMap(maxPublishers: 3) { n in n == 1 ? .fail(.boom) : values(20) }
        }
        #expect(lost == 0)
    }

    @Test func switchToLatestEndsWithExactlyOneFailure() async {
        let lost = await lostFailures {
            Publisher<Publisher<Int, OrderingError>, OrderingError>
                .just(values(20))
                .append(.fail(.boom))
                .switchToLatest()
        }
        #expect(lost == 0)
    }
}

private func values(_ count: Int) -> Publisher<Int, OrderingError> {
    Publisher<Int, Never>.sequence(0..<count).setFailureType(to: OrderingError.self)
}
