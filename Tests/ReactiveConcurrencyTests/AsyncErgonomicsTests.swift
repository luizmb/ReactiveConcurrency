@testable import ReactiveConcurrency
import Testing

private enum AErr: Error, Equatable { case boom }

@Suite struct AsyncErgonomicsTests {
    // MARK: - for-await via .values / .results

    @Test func valuesIteratesOutput() async {
        var out: [Int] = []
        for await v in Publisher<Int, Never>.sequence(1...3).values { out.append(v) }
        #expect(out == [1, 2, 3])
    }

    @Test func resultsIteratesEventsIncludingFailure() async {
        var out: [Result<Int, AErr>] = []
        for await r in Publisher<Int, AErr>.fail(.boom).results { out.append(r) }
        #expect(out == [.failure(.boom)])
    }

    // MARK: - single-shot await

    @Test func firstValueAwaitsFirst() async {
        #expect(await Publisher<Int, Never>.sequence(10...20).firstValue() == 10)
        #expect(await Publisher<Int, Never>.empty().firstValue() == nil)
    }

    @Test func firstResultAwaitsFirstEvent() async {
        #expect(await Publisher<Int, AErr>.just(7).firstResult() == .success(7))
        #expect(await Publisher<Int, AErr>.fail(.boom).firstResult() == .failure(.boom))
    }

    // MARK: - lazy DeferredTask forms

    @Test func firstValueTaskIsLazyAndComposable() async {
        let task = Publisher<Int, Never>.sequence(1...3).firstValueTask()
        #expect(await task.run() == 1)
    }

    @Test func firstResultTaskIsLazy() async {
        let task = Publisher<Int, AErr>.just(5).firstResultTask()
        #expect(await task.run() == .success(5))
    }

    // MARK: - AsyncStream -> Publisher

    @Test func asyncStreamErasesToPublisher() async {
        let stream = AsyncStream<Int> { c in c.yield(1); c.yield(2); c.yield(3); c.finish() }
        var out: [Int] = []
        for await v in stream.eraseToPublisher().values { out.append(v) }
        #expect(out == [1, 2, 3])
    }
}
