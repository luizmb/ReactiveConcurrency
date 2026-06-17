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

@Suite struct FilteringOperatorTests {
    // MARK: first / last

    @Test func firstEmitsOnlyFirstValue() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).first()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1])
    }

    @Test func firstWhereFindsFirst() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).first(where: { $0 > 3 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [4])
    }

    @Test func lastEmitsOnlyLastValue() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).last()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [5])
    }

    @Test func lastWhereFindsLast() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).last(where: { $0 < 4 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [3])
    }

    // MARK: prefix / dropFirst

    @Test func prefixTakesFirstN() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...10).prefix(3)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }

    @Test func prefixWhileStopsWhenFalse() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...10).prefix(while: { $0 < 4 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }

    @Test func dropFirstSkipsN() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).dropFirst(2)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [3, 4, 5])
    }

    @Test func dropWhileSkipsWhileTrue() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5).drop(while: { $0 < 3 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [3, 4, 5])
    }

    // MARK: output(at:) / output(in:)

    @Test func outputAtIndex() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(10...15).output(at: 2)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [12])
    }

    @Test func outputInRange() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(0...9).output(in: 2..<5)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [2, 3, 4])
    }

    // MARK: ignoreOutput

    @Test func ignoreOutputDropsValues() async {
        var sawValue = false
        var completed = false
        for await r in Publisher<Int, Never>.sequence(1...5).ignoreOutput()._stream {
            switch r {
            case .success: sawValue = true
            case .failure: break
            }
        }
        completed = true
        #expect(!sawValue)
        #expect(completed)
    }

    // MARK: removeDuplicates

    @Test func removeDuplicatesFiltersConsecutive() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence([1, 1, 2, 2, 3, 1]).removeDuplicates()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3, 1])
    }

    @Test func removeDuplicatesByPredicate() async {
        var result: [Int] = []
        // Treat values differing by less than 3 as duplicates
        for await r in Publisher<Int, Never>.sequence([1, 2, 5, 6, 10])
            .removeDuplicates(by: { abs($0 - $1) < 3 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 5, 10])
    }

    // MARK: contains / allSatisfy

    @Test func containsFindsValue() async {
        var result: [Bool] = []
        for await r in Publisher<Int, Never>.sequence(1...5).contains(3)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [true])
    }

    @Test func containsReturnsFalseWhenAbsent() async {
        var result: [Bool] = []
        for await r in Publisher<Int, Never>.sequence(1...5).contains(9)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [false])
    }

    @Test func allSatisfyTrueWhenAllMatch() async {
        var result: [Bool] = []
        for await r in Publisher<Int, Never>.sequence(2...6).allSatisfy({ $0 > 0 })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [true])
    }

    @Test func allSatisfyFalseWhenOneFails() async {
        var result: [Bool] = []
        for await r in Publisher<Int, Never>.sequence(1...5).allSatisfy({ $0.isMultiple(of: 2) })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [false])
    }

    // MARK: min / max

    @Test func minEmitsSmallest() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence([3, 1, 4, 1, 5]).min()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1])
    }

    @Test func maxEmitsLargest() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence([3, 1, 4, 1, 5]).max()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [5])
    }

    @Test func minByCustomOrder() async {
        var result: [String] = []
        for await r in Publisher<String, Never>.sequence(["bb", "a", "ccc"])
            .min(by: { $0.count < $1.count })._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == ["a"])
    }

    @Test func emptyStreamProducesNoMinMax() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.empty().min()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.isEmpty)
    }
}

@Suite struct ReplaceEmptyTests {
    @Test func replaceEmptyEmitsDefaultWhenStreamIsEmpty() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.empty().replaceEmpty(with: 42)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [42])
    }

    @Test func replaceEmptyPassesThroughWhenNotEmpty() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...3).replaceEmpty(with: 99)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }
}

@Suite struct KeyPathMapTests {
    struct Point: Sendable { let x: Int; let y: Int }

    @Test func mapSingleKeyPath() async {
        var result: [Int] = []
        let points = [Point(x: 1, y: 10), Point(x: 2, y: 20)]
        for await r in Publisher<Point, Never>.sequence(points).map(\.x)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2])
    }

    @Test func mapTwoKeyPaths() async {
        var result: [(Int, Int)] = []
        let points = [Point(x: 1, y: 10), Point(x: 2, y: 20)]
        for await r in Publisher<Point, Never>.sequence(points).map(\.x, \.y)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.map(\.0) == [1, 2])
        #expect(result.map(\.1) == [10, 20])
    }

    @Test func mapThreeKeyPaths() async {
        struct Triple: Sendable { let a: Int; let b: Int; let c: Int }
        var result: [(Int, Int, Int)] = []
        let items = [Triple(a: 1, b: 2, c: 3)]
        for await r in Publisher<Triple, Never>.sequence(items).map(\.a, \.b, \.c)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.count == 1)
        #expect(result[0].0 == 1 && result[0].1 == 2 && result[0].2 == 3)
    }
}

@Suite struct CountCollectTests {
    @Test func countEmitsTotalCount() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...7).count()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [7])
    }

    @Test func countOfEmptyIsZero() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.empty().count()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [0])
    }

    @Test func collectByCountGroupsElements() async {
        var result: [[Int]] = []
        for await r in Publisher<Int, Never>.sequence(1...7).collect(3)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [[1, 2, 3], [4, 5, 6], [7]])
    }
}

@Suite struct SetFailureTypeTests {
    @Test func setFailureTypeWidensInfallible() async {
        enum MyError: Error { case boom }
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...3)
            .setFailureType(to: MyError.self)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }

    @Test func replaceNilSubstitutesOptionals() async {
        var result: [Int] = []
        for await r in Publisher<Int?, Never>.sequence([1, nil, 3, nil, 5])
            .replaceNil(with: 0)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 0, 3, 0, 5])
    }
}

@Suite struct EncodeDecodeTests {
    enum CodecError: Error, Equatable { case encodingFailed; case decodingFailed }

    @Test func encodeTransformsValuesToData() async {
        let encoder: @Sendable (Int) -> Result<Data, CodecError> = { n in
            var value = Int32(n).bigEndian
            return .success(Data(bytes: &value, count: 4))
        }
        var result: [Data] = []
        for await r in Publisher<Int, Never>.sequence([1, 2, 3])
            .encode(encoder: encoder)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result.count == 3)
        #expect(result[0] == Data([0, 0, 0, 1]))
    }

    @Test func encodePropagatesEncoderFailure() async {
        let encoder: @Sendable (Int) -> Result<Data, CodecError> = { _ in .failure(.encodingFailed) }
        var result: [CodecError] = []
        for await r in Publisher<Int, Never>.sequence([1])
            .encode(encoder: encoder)._stream {
            if case .failure(let e) = r { result.append(e) }
        }
        #expect(result == [.encodingFailed])
    }

    @Test func decodeTransformsDataToValues() async {
        let decoder: @Sendable (Data) -> Result<Int, CodecError> = { data in
            guard data.count == 4 else { return .failure(.decodingFailed) }
            let value = data.withUnsafeBytes { $0.load(as: Int32.self).bigEndian }
            return .success(Int(value))
        }
        var value1 = Int32(42).bigEndian
        let encoded = [Data(bytes: &value1, count: 4)]
        var result: [Int] = []
        for await r in Publisher<Data, Never>.sequence(encoded)
            .decode(decoder: decoder)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [42])
    }

    @Test func decodeRoundTrips() async {
        let encoder: @Sendable (Int) -> Result<Data, CodecError> = { n in
            var v = Int32(n).bigEndian; return .success(Data(bytes: &v, count: 4))
        }
        let decoder: @Sendable (Data) -> Result<Int, CodecError> = { d in
            .success(Int(d.withUnsafeBytes { $0.load(as: Int32.self).bigEndian }))
        }
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence([10, 20, 30])
            .encode(encoder: encoder)
            .decode(decoder: decoder)._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [10, 20, 30])
    }

    @Test func tryMapResultIntroducesErrorOnNeverPublisher() async {
        let stream = Publisher<Int, Never>.sequence([1, -1, 2])
            .tryMap { n -> Result<Int, CodecError> in
                n < 0 ? .failure(.encodingFailed) : .success(n * 10)
            }._stream
        var values: [Int] = []
        var errors: [CodecError] = []
        for await r in stream {
            switch r {
            case .success(let v): values.append(v)
            case .failure(let e): errors.append(e)
            }
        }
        #expect(values == [10])
        #expect(errors == [.encodingFailed])
    }
}

@Suite struct RecordTests {
    @Test func recordReplaysValuesAndCompletion() async {
        let record = Record<Int, Never>(recording: .init(output: [10, 20, 30]))
        var result: [Int] = []
        for await r in record.eraseToPublisher()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [10, 20, 30])
    }

    @Test func recordBuilderClosure() async {
        let record = Record<Int, Never> { recording in
            recording.receive(1)
            recording.receive(2)
            recording.receive(completion: .finished)
        }
        var result: [Int] = []
        for await r in record.eraseToPublisher()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2])
    }

    @Test func recordIsReplayedOnEachSubscription() async {
        let record = Record<Int, Never>(recording: .init(output: [7, 8]))
        var first: [Int] = []
        var second: [Int] = []
        for await r in record.eraseToPublisher()._stream {
            if case .success(let v) = r { first.append(v) }
        }
        for await r in record.eraseToPublisher()._stream {
            if case .success(let v) = r { second.append(v) }
        }
        #expect(first == [7, 8])
        #expect(second == [7, 8])
    }
}

@Suite struct TryOperatorTests {
    enum TestError: Error, Equatable { case bad }

    @Test func tryFilterPassesMatchingValues() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...5)
            .tryFilter { v throws(TestError) in
                if v == 3 { throw TestError.bad }
                return v % 2 == 1
            }._stream {
            switch r {
            case .success(let v): result.append(v)
            case .failure: result.append(-1)
            }
        }
        #expect(result == [1, -1])
    }

    @Test func tryCompactMapFiltersAndTransforms() async {
        var result: [String] = []
        for await r in Publisher<Int, Never>.sequence(1...4)
            .tryCompactMap { v throws(TestError) -> String? in
                if v == 2 { throw TestError.bad }
                return v.isMultiple(of: 2) ? nil : "\(v)"
            }._stream {
            switch r {
            case .success(let v): result.append(v)
            case .failure: result.append("err")
            }
        }
        #expect(result == ["1", "err"])
    }

    @Test func tryScanAccumulates() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...4)
            .tryScan(0) { acc, v throws(TestError) in
                if v == 3 { throw TestError.bad }
                return acc + v
            }._stream {
            switch r {
            case .success(let v): result.append(v)
            case .failure: result.append(-99)
            }
        }
        #expect(result == [1, 3, -99])
    }

    @Test func tryReduceEmitsFinalOrError() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...3)
            .tryReduce(0) { acc, v throws(TestError) -> Int in acc + v }._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [6])
    }

    @Test func tryCatchRecoversWithPublisher() async {
        let result = Collector<Int>()
        let subject = PassthroughSubject<Int, TestError>()
        let recovery: Publisher<Int, TestError> = Publisher<Int, Never>.sequence([99, 100])
            .setFailureType(to: TestError.self)
        let cancellable = subject.eraseToPublisher()
            .tryCatch { _ throws(TestError) in recovery }
            .sink(
                receiveCompletion: { _ in },
                receiveValue: { result.append($0) }
            )
        subject.send(1)
        subject.send(completion: .failure(.bad))
        await poll(until: { result.values.count == 3 })
        cancellable.cancel()
        #expect(result.values == [1, 99, 100])
    }

    @Test func assertNoFailurePassesValues() async {
        var result: [Int] = []
        for await r in Publisher<Int, Never>.sequence(1...3).assertNoFailure()._stream {
            if case .success(let v) = r { result.append(v) }
        }
        #expect(result == [1, 2, 3])
    }
}
