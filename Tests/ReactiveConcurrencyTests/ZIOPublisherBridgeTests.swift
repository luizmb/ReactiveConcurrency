@testable import ReactiveConcurrency
import Testing

private enum ZErr: Error, Equatable { case boom }

private func events<O: Sendable, F: Error>(_ publisher: Publisher<O, F>) async -> [Result<O, F>] {
    var out: [Result<O, F>] = []
    for await result in publisher._stream { out.append(result) }
    return out
}

@Suite struct ZIOPublisherBridgeTests {
    @Test func successEmitsValueThenFinishes() async {
        let zio = ZIO<Int, Int, ZErr>.pure(42)
        #expect(await events(zio.eraseToPublisher(environment: 0)) == [.success(42)])
    }

    @Test func failurePropagates() async {
        let zio = ZIO<Int, Int, ZErr> { _ in DeferredTask { .failure(.boom) } }
        #expect(await events(zio.eraseToPublisher(environment: 0)) == [.failure(.boom)])
    }

    @Test func usesProvidedEnvironment() async {
        let zio = ZIO<Int, Int, ZErr> { env in DeferredTask { .success(env * 2) } }
        #expect(await events(zio.eraseToPublisher(environment: 21)) == [.success(42)])
    }

    @Test func voidEnvironmentConvenience() async {
        let zio = ZIO<Void, String, ZErr> { _ in DeferredTask { .success("hi") } }
        #expect(await events(zio.eraseToPublisher()) == [.success("hi")])
    }

    @Test func coldReRunsPerSubscription() async {
        let counter = AtomicCounter()
        let zio = ZIO<Int, Int, ZErr> { _ in
            DeferredTask { counter.increment(); return .success(1) }
        }
        let publisher = zio.eraseToPublisher(environment: 0)
        _ = await events(publisher)
        _ = await events(publisher)
        #expect(counter.current == 2)
    }
}
