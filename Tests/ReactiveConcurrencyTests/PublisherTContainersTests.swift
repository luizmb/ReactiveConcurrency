// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

private enum CErr: Error, Equatable { case boom }

private func vals<O: Sendable, F: Error>(_ publisher: Publisher<O, F>) async -> [O] {
    var out: [O] = []
    for await result in publisher._stream {
        if case let .success(v) = result { out.append(v) }
    }
    return out
}

// MARK: - PublisherTArray

@Suite(.timeLimit(.minutes(1))) struct PublisherTArrayTests {
    @Test func mapT() async {
        #expect(await vals(mapTPublisherArray({ $0 * 2 }, Publisher<[Int], Never>.just([1, 2, 3]))) == [[2, 4, 6]])
        #expect(await vals({ $0 * 2 } <£^> Publisher<[Int], Never>.just([1, 2])) == [[2, 4]])
    }

    @Test func applyTCartesian() async {
        let fns = Publisher<[@Sendable (Int) -> Int], Never>.just([{ $0 + 1 }, { $0 * 10 }])
        let vs = Publisher<[Int], Never>.just([1, 2])
        #expect(await vals(applyTPublisherArray(fns, vs)) == [[2, 3, 10, 20]])
    }

    @Test func flatMapTConcat() async {
        let p = Publisher<[Int], Never>.just([1, 2])
        let chained = p >>- { n in Publisher<[Int], Never>.just([n, n * 10]) }
        #expect(await vals(chained) == [[1, 10, 2, 20]])
    }
}

// MARK: - PublisherTOptional

@Suite(.timeLimit(.minutes(1))) struct PublisherTOptionalTests {
    @Test func mapT() async {
        #expect(await vals(mapTPublisherOptional({ $0 * 2 }, Publisher<Int?, Never>.just(3))) == [6])
        #expect(await vals(mapTPublisherOptional({ $0 * 2 }, Publisher<Int?, Never>.just(nil))) == [nil])
    }

    @Test func flatMapT() async {
        let some = Publisher<Int?, Never>.just(5) >>- { n in Publisher<Int?, Never>.just(n * 2) }
        let none = Publisher<Int?, Never>.just(nil) >>- { n in Publisher<Int?, Never>.just(n * 2) }
        #expect(await vals(some) == [10])
        #expect(await vals(none) == [nil])
    }

    @Test func alternativeFirstNonNil() async {
        #expect(await vals(altPublisherOptional(Publisher<Int?, Never>.just(nil), Publisher<Int?, Never>.just(7))) == [7])
        #expect(await vals(altPublisherOptional(Publisher<Int?, Never>.just(3), Publisher<Int?, Never>.just(9))) == [3])
    }
}

// MARK: - PublisherTResult

@Suite(.timeLimit(.minutes(1))) struct PublisherTResultTests {
    @Test func mapT() async {
        let ok = Publisher<Result<Int, CErr>, Never>.just(.success(5))
        let bad = Publisher<Result<Int, CErr>, Never>.just(.failure(.boom))
        #expect(await vals(mapTPublisherResult({ $0 * 2 }, ok)) == [.success(10)])
        #expect(await vals(mapTPublisherResult({ $0 * 2 }, bad)) == [.failure(.boom)])
    }

    @Test func flatMapTShortCircuits() async {
        let ok = Publisher<Result<Int, CErr>, Never>.just(.success(5))
        let chained = ok >>- { n in Publisher<Result<Int, CErr>, Never>.just(.success(n + 1)) }
        let bad = Publisher<Result<Int, CErr>, Never>.just(.failure(.boom))
        let chainedBad = bad >>- { n in Publisher<Result<Int, CErr>, Never>.just(.success(n + 1)) }
        #expect(await vals(chained) == [.success(6)])
        #expect(await vals(chainedBad) == [.failure(.boom)])
    }

    @Test func alternativeFirstSuccess() async {
        let bad = Publisher<Result<Int, CErr>, Never>.just(.failure(.boom))
        let ok7 = Publisher<Result<Int, CErr>, Never>.just(.success(7))
        let ok3 = Publisher<Result<Int, CErr>, Never>.just(.success(3))
        let ok9 = Publisher<Result<Int, CErr>, Never>.just(.success(9))
        #expect(await vals(altPublisherResult(bad, ok7)) == [.success(7)])
        #expect(await vals(altPublisherResult(ok3, ok9)) == [.success(3)])
    }
}

// MARK: - PublisherTValidation

@Suite(.timeLimit(.minutes(1))) struct PublisherTValidationTests {
    private func tag(_ v: Validation<[String], Int>) -> String {
        switch v {
        case let .success(a): "S\(a)"
        case let .failure(e): "F\(e.joined(separator: ","))"
        }
    }

    @Test func mapTOverSuccess() async {
        let p = Publisher<Validation<[String], Int>, Never>.just(.success(3))
        let mapped = await vals(mapTPublisherValidation({ $0 * 2 }, p)).map(tag)
        #expect(mapped == ["S6"])
    }

    @Test func applyTAccumulatesErrors() async {
        let fns = Publisher<Validation<[String], @Sendable (Int) -> Int>, Never>.just(.failure(["e1"]))
        let vs = Publisher<Validation<[String], Int>, Never>.just(.failure(["e2"]))
        let result = await vals(applyTPublisherValidation(fns, vs)).map(tag)
        #expect(result == ["Fe1,e2"])
    }
}
