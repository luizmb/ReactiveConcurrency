// SPDX-License-Identifier: Apache-2.0

import CoreFPOperators
import Foundation
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import Testing

private enum TestError: Error, Equatable { case boom }
private enum MappedError: Error, Equatable { case mapped }

// Collect every value (ignoring failures) from a never-failing publisher.
private func values<O: Sendable>(_ publisher: Publisher<O, Never>) async -> [O] {
    var out: [O] = []
    for await result in publisher._stream {
        if case let .success(v) = result { out.append(v) }
    }
    return out
}

// Collect the raw Result events from a failable publisher.
private func events<O: Sendable, E: Error>(_ publisher: Publisher<O, E>) async -> [Result<O, E>] {
    var out: [Result<O, E>] = []
    for await result in publisher._stream {
        out.append(result)
    }
    return out
}

// MARK: - Functor

@Suite struct PublisherFunctorTests {
    @Test func staticFmap() async {
        let f = Publisher<Int, Never>.fmap { $0 * 2 }
        #expect(await values(f(.sequence(1...3))) == [2, 4, 6])
    }

    @Test func replaceAndVoid() async {
        #expect(await values(Publisher<Int, Never>.sequence(1...3).replace("x")) == ["x", "x", "x"])
        #expect(await values(Publisher<Int, Never>.sequence(1...3).void()).count == 3)
    }

    @Test func bimapTransformsBothChannels() async {
        let p = Publisher<Int, TestError>.fail(.boom).bimap(
            transformOutput: { $0 + 1 },
            transformError: { _ in MappedError.mapped }
        )
        #expect(await events(p) == [.failure(.mapped)])
    }

    @Test func functorOperators() async {
        let mapped = { (x: Int) in x * 10 } <£> Publisher<Int, Never>.sequence(1...3)
        #expect(await values(mapped) == [10, 20, 30])
        #expect(await values(Publisher<Int, Never>.sequence(1...3) <&> { $0 + 1 }) == [2, 3, 4])
        #expect(await values(Publisher<Int, Never>.sequence(1...2) £> 9) == [9, 9])
        #expect(await values(7 <£ Publisher<Int, Never>.sequence(1...2)) == [7, 7])
    }
}

// MARK: - Applicative

@Suite struct PublisherApplicativeTests {
    @Test func pureEmitsSingleValue() async {
        #expect(await values(Publisher<Int, Never>.pure(5)) == [5])
    }

    @Test func seqRightAndSeqLeft() async {
        let a = Publisher<Int, Never>.sequence(1...3)
        let b = Publisher<String, Never>.sequence(["a", "b", "c"])
        #expect(await values(a.seqRight(b)) == ["a", "b", "c"])
        #expect(await values(a.seqLeft(b)) == [1, 2, 3])
    }

    @Test func applyOperator() async {
        let fnList: [@Sendable (Int) -> Int] = [{ $0 + 1 }, { $0 * 2 }]
        let fns = Publisher<@Sendable (Int) -> Int, Never>.sequence(fnList)
        let vals = Publisher<Int, Never>.sequence([10, 20])
        #expect(await values(fns <*> vals) == [11, 40])
    }

    @Test func seqOperators() async {
        let a = Publisher<Int, Never>.sequence(1...2)
        let b = Publisher<String, Never>.sequence(["x", "y"])
        #expect(await values(a *> b) == ["x", "y"])
        #expect(await values(a <* b) == [1, 2])
    }
}

// MARK: - Monad

@Suite struct PublisherMonadTests {
    @Test func staticFlatMapAndKleisli() async {
        // flatMap merges inner publishers concurrently, so compare as a multiset.
        let twice = Publisher<Int, Never>.flatMap { Publisher<Int, Never>.sequence([$0, $0]) }
        #expect(await values(twice(.sequence(1...2))).sorted() == [1, 1, 2, 2])

        let k = Publisher<Int, Never>.kleisli(
            { Publisher<Int, Never>.just($0 + 1) },
            { Publisher<Int, Never>.just($0 * 10) }
        )
        #expect(await values(k(4)) == [50])
    }

    @Test func joinFlattens() async {
        let nested = Publisher<Publisher<Int, Never>, Never>.sequence([.just(1), .just(2)])
        #expect(await values(nested.join()).sorted() == [1, 2])
        #expect(await values(Publisher<Publisher<Int, Never>, Never>.join(.just(.just(9)))) == [9])
    }

    @Test func monadOperators() async {
        let bound = Publisher<Int, Never>.just(3) >>- { Publisher<Int, Never>.just($0 + 1) }
        #expect(await values(bound) == [4])
        let bound2 = { (x: Int) in Publisher<Int, Never>.just(x * 2) } -<< Publisher<Int, Never>.just(5)
        #expect(await values(bound2) == [10])
        let composed = { (x: Int) in Publisher<Int, Never>.just(x + 1) }
            >=> { (x: Int) in Publisher<Int, Never>.just(x * 3) }
        #expect(await values(composed(2)) == [9])
    }
}

// MARK: - Alternative

@Suite struct PublisherAlternativeTests {
    @Test func altConcatenates() async {
        let a = Publisher<Int, Never>.sequence(1...2)
        let b = Publisher<Int, Never>.sequence(3...4)
        #expect(await values(a.alt(b)) == [1, 2, 3, 4])
        #expect(await values(a <|> b) == [1, 2, 3, 4])
    }

    @Test func altShortCircuitsOnFailure() async {
        let a = Publisher<Int, TestError>.fail(.boom)
        let b = Publisher<Int, TestError>.just(99)
        #expect(await events(a <|> b) == [.failure(.boom)])
    }
}

// MARK: - Future

@Suite struct FutureTests {
    @Test func futureThrowingSuccess() async {
        let p = Publisher<Int, TestError>.future { 42 }
        #expect(await events(p) == [.success(42)])
    }

    @Test func futureThrowingFailure() async {
        let p = Publisher<Int, TestError>.future { () async throws(TestError) -> Int in throw TestError.boom }
        #expect(await events(p) == [.failure(.boom)])
    }

    @Test func futureResultForm() async {
        let ok = Publisher<Int, TestError>.future { Result<Int, TestError>.success(7) }
        let bad = Publisher<Int, TestError>.future { Result<Int, TestError>.failure(.boom) }
        #expect(await events(ok) == [.success(7)])
        #expect(await events(bad) == [.failure(.boom)])
    }
}

// MARK: - DeferredTask <-> Publisher bridges

@Suite struct DeferredTaskBridgeTests {
    @Test func taskToPublisher() async {
        #expect(await values(DeferredTask { 11 }.eraseToPublisher()) == [11])
    }

    @Test func resultTaskToThrowingPublisher() async {
        let ok = DeferredTask<Result<Int, TestError>> { .success(3) }.eraseToThrowingPublisher()
        let bad = DeferredTask<Result<Int, TestError>> { .failure(.boom) }.eraseToThrowingPublisher()
        #expect(await events(ok) == [.success(3)])
        #expect(await events(bad) == [.failure(.boom)])
    }

    @Test func publisherFirstValue() async {
        let first = await Publisher<Int, Never>.sequence(1...5).firstValue()
        #expect(first == 1)
        let empty = await Publisher<Int, Never>.empty().firstValue()
        #expect(empty == nil)
    }

    @Test func publisherFirstResult() async {
        let first = await Publisher<Int, TestError>.just(8).firstResult()
        #expect(first == .success(8))
        let failed = await Publisher<Int, TestError>.fail(.boom).firstResult()
        #expect(failed == .failure(.boom))
    }

    @Test func taskRoundTrip() async {
        let roundTripped = await DeferredTask { 99 }.eraseToPublisher().firstValue()
        #expect(roundTripped == 99)
    }
}

// MARK: - DeferredStream <-> Publisher bridges

@Suite struct DeferredStreamBridgeTests {
    @Test func streamToPublisher() async {
        #expect(await values(DeferredStream<Int>.pure(5).eraseToPublisher()) == [5])
    }

    @Test func resultStreamToThrowingPublisher() async {
        let stream = DeferredStream<Result<Int, TestError>>.wrap(
            AsyncStream { c in
                c.yield(.success(1)); c.yield(.failure(.boom)); c.finish()
            }
        )
        #expect(await events(stream.eraseToThrowingPublisher()) == [.success(1), .failure(.boom)])
    }

    @Test func publisherToDeferredStream() async {
        var out: [Int] = []
        for await v in Publisher<Int, Never>.sequence(1...3).values {
            out.append(v)
        }
        #expect(out == [1, 2, 3])
    }

    @Test func publisherToResultStream() async {
        var out: [Result<Int, TestError>] = []
        for await r in Publisher<Int, TestError>.just(7).results {
            out.append(r)
        }
        #expect(out == [.success(7)])
    }

    @Test func streamRoundTrip() async {
        var out: [Int] = []
        for await v in DeferredStream<Int>.pure(42).eraseToPublisher().values {
            out.append(v)
        }
        #expect(out == [42])
    }
}

// MARK: - Result / Optional conveniences

@Suite struct ConveniencePublisherTests {
    @Test func resultPublisher() async {
        #expect(await events(Result<Int, TestError>.success(1).publisher) == [.success(1)])
        #expect(await events(Result<Int, TestError>.failure(.boom).publisher) == [.failure(.boom)])
    }

    @Test func optionalPublisher() async {
        #expect(await values(Int?.some(5).publisher) == [5])
        #expect(await values(Int?.none.publisher).isEmpty)
    }
}
