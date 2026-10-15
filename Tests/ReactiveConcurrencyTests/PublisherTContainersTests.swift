// SPDX-License-Identifier: Apache-2.0

import CoreFP
import CoreFPOperators
import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import ReactiveConcurrencyTransformers
import Testing

// The stack names are qualified: on Apple, FP declares Combine stacks with the same names.
private typealias PArray<A: Sendable> = ReactiveConcurrencyTransformers.PublisherTArray<Never, A>
private typealias POptional<A: Sendable> = ReactiveConcurrencyTransformers.PublisherTOptional<Never, A>
private typealias PResult<A: Sendable> = ReactiveConcurrencyTransformers.PublisherTResult<Never, CErr, A>
private typealias PValidation<A: Sendable> = ReactiveConcurrencyTransformers.PublisherTValidation<Never, [String], A>

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
    @Test func map() async {
        #expect(await vals(PArray(.just([1, 2, 3])).map { $0 * 2 }.rawValue) == [[2, 4, 6]])
        #expect(await vals(({ $0 * 2 } <£> Publisher<[Int], Never>.just([1, 2]).publisherT).rawValue) == [[2, 4]])
    }

    @Test func applyCartesian() async {
        let fns = PArray<@Sendable (Int) -> Int>(.just([{ $0 + 1 }, { $0 * 10 }]))
        let vs = PArray(.just([1, 2]))
        #expect(await vals((fns <*> vs).rawValue) == [[2, 3, 10, 20]])
    }
}

// MARK: - PublisherTOptional

@Suite(.timeLimit(.minutes(1))) struct PublisherTOptionalTests {
    @Test func map() async {
        #expect(await vals(POptional(.just(3)).map { $0 * 2 }.rawValue) == [6])
        #expect(await vals(POptional(.just(nil)).map { $0 * 2 }.rawValue) == [nil])
    }

    @Test func flatMap() async {
        let some = POptional(.just(5)) >>- { n in POptional(.just(n * 2)) }
        let none = POptional(.just(nil)) >>- { n in POptional(.just(n * 2)) }
        #expect(await vals(some.rawValue) == [10])
        #expect(await vals(none.rawValue) == [nil])
    }

    @Test func zipAltFirstNonNil() async {
        #expect(await vals(POptional(.just(nil)).zipAlt(POptional(.just(7))).rawValue) == [7])
        #expect(await vals(POptional(.just(3)).zipAlt(POptional(.just(9))).rawValue) == [3])
    }
}

// MARK: - PublisherTResult

@Suite(.timeLimit(.minutes(1))) struct PublisherTResultTests {
    @Test func map() async {
        #expect(await vals(PResult(.just(.success(5))).map { $0 * 2 }.rawValue) == [.success(10)])
        #expect(await vals(PResult(.just(.failure(.boom))).map { $0 * 2 }.rawValue) == [.failure(.boom)])
    }

    @Test func flatMapShortCircuits() async {
        let chained = PResult(.just(.success(5))) >>- { n in PResult(.just(.success(n + 1))) }
        let chainedBad = PResult(.just(.failure(.boom))) >>- { n in PResult(.just(.success(n + 1))) }
        #expect(await vals(chained.rawValue) == [.success(6)])
        #expect(await vals(chainedBad.rawValue) == [.failure(.boom)])
    }

    @Test func zipAltFirstSuccess() async {
        #expect(await vals(PResult(.just(.failure(.boom))).zipAlt(PResult(.just(.success(7)))).rawValue) == [.success(7)])
        #expect(await vals(PResult(.just(.success(3))).zipAlt(PResult(.just(.success(9)))).rawValue) == [.success(3)])
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

    @Test func mapOverSuccess() async {
        let mapped = await vals(PValidation(.just(.success(3))).map { $0 * 2 }.rawValue).map(tag)
        #expect(mapped == ["S6"])
    }

    @Test func applyAccumulatesErrors() async {
        let fns = PValidation<@Sendable (Int) -> Int>(.just(.failure(["e1"])))
        let vs = PValidation<Int>(.just(.failure(["e2"])))
        #expect(await vals((fns <*> vs).rawValue).map(tag) == ["Fe1,e2"])
    }
}
