// SPDX-License-Identifier: Apache-2.0

// FP-parity surface for Publisher: static curried Functor/Applicative/Monad/Alternative
// forms plus the instance combinators (bimap, void, alt, join, seq, replace) that the
// symbolic operators in ReactiveConcurrencyOperators build on. Mirrors the shapes used by
// DeferredStream / DeferredTask so the two compose with the same vocabulary.

// MARK: - Functor

public extension Publisher {
    // replace :: Publisher a e -> b -> Publisher b e
    func replace<B: Sendable>(_ value: B) -> Publisher<B, Failure> {
        map { _ in value }
    }

    // void :: Publisher a e -> Publisher () e
    func void() -> Publisher<Void, Failure> {
        map { _ in }
    }

    // bimap :: (a -> b) -> (e -> f) -> Publisher a e -> Publisher b f
    func bimap<T: Sendable, E: Error>(
        transformOutput: @escaping @Sendable (Output) -> T,
        transformError: @escaping @Sendable (Failure) -> E
    ) -> Publisher<T, E> {
        map(transformOutput).mapError(transformError)
    }

    // fmap :: (a -> b) -> Publisher a e -> Publisher b e
    static func fmap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> T
    ) -> @Sendable (Publisher<Output, Failure>) -> Publisher<T, Failure> {
        { @Sendable publisher in publisher.map(transform) }
    }
}

// MARK: - Zippy Semigroupal (ZipList-style)

//
// `apply`/`seqLeft`/`seqRight` here form a *zippy* product: they pair elements positionally
// (like ZipList) and truncate at the shorter side. This is the product users want from a
// stream — but it is NOT the Applicative derived from the monad (`flatMap`, which is a
// cartesian merge). In particular the Applicative *identity* law `pure(id) <*> v == v` fails
// for |v| > 1, because `pure` yields a single element and the zip truncates `v` to length 1.
// Treat these as a zippy Semigroupal (`zip`/`zipWith`), not a lawful Applicative; reach for
// `flatMap` when you want the cartesian, monad-consistent product.

public extension Publisher {
    // pure :: a -> Publisher a e — single-element (ZipList pure is the identity element for
    // the zippy product only up to length 1; a lawful ZipList pure would repeat infinitely).
    static func pure(_ value: Output) -> Publisher<Output, Failure> {
        just(value)
    }

    // seqRight :: Publisher a e -> Publisher b e -> Publisher b e (zippy: pairs positionally)
    func seqRight<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<B, Failure> {
        zip(rhs).map { _, b in b }
    }

    // seqLeft :: Publisher a e -> Publisher b e -> Publisher a e (zippy: pairs positionally)
    func seqLeft<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<Output, Failure> {
        zip(rhs).map { a, _ in a }
    }

    // zip :: Publisher a e -> Publisher b e -> Publisher (a, b) e
    static func zip<B: Sendable>(
        _ pa: Publisher<Output, Failure>,
        _ pb: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        pa.zip(pb)
    }
}

// apply :: Publisher (a -> b) e -> Publisher a e -> Publisher b e
// Zippy (ZipList-style): pairs each fn with each value positionally and truncates at the shorter
// side. NOT the cartesian, monad-consistent product — see the section note above.
public func applyPublisher<A: Sendable, B: Sendable, E: Error>(
    _ fns: Publisher<@Sendable (A) -> B, E>,
    _ values: Publisher<A, E>
) -> Publisher<B, E> {
    fns.zip(values).map { pair in pair.0(pair.1) }
}

// MARK: - Monad

public extension Publisher {
    // flatMap (static, curried) :: (a -> Publisher b e) -> Publisher a e -> Publisher b e
    static func flatMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> @Sendable (Publisher<Output, Failure>) -> Publisher<T, Failure> {
        { @Sendable publisher in publisher.flatMap(transform) }
    }

    // join :: Publisher (Publisher a e) e -> Publisher a e
    func join<A: Sendable>() -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        flatMap { $0 }
    }

    // join (static) :: Publisher (Publisher a e) e -> Publisher a e
    static func join<A: Sendable>(
        _ nested: Publisher<Publisher<A, Failure>, Failure>
    ) -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        nested.flatMap { $0 }
    }

    // kleisli :: (a -> Publisher b e) -> (b -> Publisher c e) -> (a -> Publisher c e)
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Output) -> Publisher<B, Failure>,
        _ g: @escaping @Sendable (B) -> Publisher<C, Failure>
    ) -> @Sendable (Output) -> Publisher<C, Failure> {
        { @Sendable a in f(a).flatMap(g) }
    }
}

// MARK: - Alternative

public extension Publisher {
    // alt :: Publisher a e -> Publisher a e -> Publisher a e
    // Concatenation: emit every value from self, then (only if self finished without failing)
    // every value from other. A failure on either side propagates and seals the stream.
    func alt(_ other: Publisher<Output, Failure>) -> Publisher<Output, Failure> {
        let otherFactory = other._stream.factory
        return _operator { downstream, upstream in
            for await result in upstream {
                switch result {
                case let .success(value):
                    if case .terminated = downstream.yield(.success(value)) { return }
                case let .failure(error):
                    _ = downstream.yield(.failure(error)); downstream.finish(); return
                }
            }
            for await result in otherFactory() {
                switch result {
                case let .success(value):
                    if case .terminated = downstream.yield(.success(value)) { return }
                case let .failure(error):
                    _ = downstream.yield(.failure(error)); downstream.finish(); return
                }
            }
            downstream.finish()
        }
    }

    // alt (static) :: Publisher a e -> Publisher a e -> Publisher a e
    static func alt(
        _ lhs: Publisher<Output, Failure>,
        _ rhs: @autoclosure () -> Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        lhs.alt(rhs())
    }
}
