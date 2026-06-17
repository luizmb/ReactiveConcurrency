import DataStructure
import ReactiveConcurrency

// ReaderTPublisher: outer = Reader, inner = Publisher
// Type: Reader<Env, Publisher<A, F>>

public extension Reader {
    func flatMapT<A: Sendable, B: Sendable, F: Error>(
        _ fn: @escaping @Sendable (A) -> Reader<Environment, Publisher<B, F>>
    ) -> Reader<Environment, Publisher<B, F>>
    where Output == Publisher<A, F>, Environment: Sendable {
        Reader<Environment, Publisher<B, F>> { env in
            self.runReader(env).flatMap(maxPublishers: 1) { a in fn(a).runReader(env) }
        }
    }
}

public func bindTReaderPublisher<Env: Sendable, A: Sendable, B: Sendable, F: Error>(
    _ reader: Reader<Env, Publisher<A, F>>,
    _ fn: @escaping @Sendable (A) -> Reader<Env, Publisher<B, F>>
) -> Reader<Env, Publisher<B, F>> {
    reader.flatMapT(fn)
}
