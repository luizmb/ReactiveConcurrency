// SPDX-License-Identifier: Apache-2.0

@testable import ReactiveConcurrency
import Testing

private func settle() async { for _ in 0..<20 {
    await Task.yield()
} }

private func poll(timeoutMs: Int = 2_000, until condition: @Sendable () -> Bool) async {
    for _ in 0..<(timeoutMs / 2) {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

private func collect<O: Sendable>(_ publisher: Publisher<O, Never>) async -> [O] {
    var out: [O] = []
    for await v in publisher.values {
        out.append(v)
    }
    return out
}

// MARK: - MergeMany

@Suite(.timeLimit(.minutes(1))) struct MergeManyTests {
    @Test func mergesAllSources() async {
        let merged = Publisher<Int, Never>.merge([.sequence(1...2), .sequence(3...4), .sequence(5...6)])
        #expect(await collect(merged).sorted() == [1, 2, 3, 4, 5, 6])
    }

    @Test func instanceMergeWithArray() async {
        let merged = Publisher<Int, Never>.just(1).merge(with: [.just(2), .just(3)])
        #expect(await collect(merged).sorted() == [1, 2, 3])
    }

    @Test func emptyArrayIsEmpty() async {
        #expect(await collect(Publisher<Int, Never>.merge([])).isEmpty)
    }
}

// MARK: - buffer

@Suite(.timeLimit(.minutes(1))) struct BufferTests {
    @Test func passesAllThroughWhenConsumerKeepsUp() async {
        let dropOldest = Publisher<Int, Never>.sequence(1...5).buffer(size: 16, whenFull: .dropOldest)
        let dropNewest = Publisher<Int, Never>.sequence(1...5).buffer(size: 16, whenFull: .dropNewest)
        #expect(await collect(dropOldest) == [1, 2, 3, 4, 5])
        #expect(await collect(dropNewest) == [1, 2, 3, 4, 5])
    }
}

// MARK: - multicast

@Suite(.timeLimit(.minutes(1))) struct MulticastTests {
    @Test func multicastThroughPassthroughFansToSubscribers() async {
        let upstream = PassthroughSubject<Int, Never>()
        let connectable = upstream.eraseToPublisher().multicast(subject: PassthroughSubject<Int, Never>())
        let v1 = Collector<Int>()
        let v2 = Collector<Int>()

        let c1 = connectable.eraseToPublisher().sink { v1.append($0) }
        let c2 = connectable.eraseToPublisher().sink { v2.append($0) }
        let connection = connectable.connect()
        await settle()
        upstream.send(1); upstream.send(2)
        await poll { v1.values.count >= 2 && v2.values.count >= 2 }
        c1.cancel(); c2.cancel(); connection.cancel()

        #expect(v1.values == [1, 2])
        #expect(v2.values == [1, 2])
    }

    @Test func multicastThroughCurrentValueReplaysLatest() async {
        let upstream = PassthroughSubject<Int, Never>()
        let connectable = upstream.eraseToPublisher().multicast { CurrentValueSubject<Int, Never>(0) }
        let connection = connectable.connect()
        await settle()
        upstream.send(1)
        await settle()

        // A late subscriber sees the replayed latest value (1), not the earlier 0.
        let late = Collector<Int>()
        let c = connectable.eraseToPublisher().sink { late.append($0) }
        await poll { !late.values.isEmpty }
        #expect(late.values.first == 1)

        c.cancel(); connection.cancel()
    }
}
