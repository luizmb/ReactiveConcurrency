// SPDX-License-Identifier: Apache-2.0

// FP-parity surface for Publisher: static curried Functor/Applicative/Monad/Alternative
// forms plus the instance combinators (bimap, void, alt, join, seq, replace) that the
// symbolic operators in ReactiveConcurrencyOperators build on. Mirrors the shapes used by
// DeferredStream / DeferredTask so the two compose with the same vocabulary.

// MARK: - Functor

public extension Publisher {
    /// Replaces every emitted value with the constant `value` (Functor `<$`).
    func replace<B: Sendable>(_ value: B) -> Publisher<B, Failure> {
        map { _ in value }
    }

    /// Discards each value, keeping only the stream's shape as `Publisher<Void, Failure>`.
    func void() -> Publisher<Void, Failure> {
        map { _ in }
    }

    /// Maps both channels at once: `transformOutput` over values and `transformError` over the
    /// failure (Bifunctor `bimap`).
    func bimap<T: Sendable, E: Error>(
        transformOutput: @escaping @Sendable (Output) -> T,
        transformError: @escaping @Sendable (Failure) -> E
    ) -> Publisher<T, E> {
        map(transformOutput).mapError(transformError)
    }

    /// Curried, free-function form of `map`: lifts `transform` into a function on publishers
    /// (Functor `fmap`), for point-free composition.
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
    /// Lifts a single value into a one-element publisher (Applicative `pure`). Note this is a
    /// single-shot element, so it acts as the zippy product's identity only up to length 1.
    static func pure(_ value: Output) -> Publisher<Output, Failure> {
        just(value)
    }

    /// Zips positionally with `rhs` and keeps the right value at each position (`*>`); truncates
    /// to the shorter side.
    func seqRight<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<B, Failure> {
        zip(rhs).map { _, b in b }
    }

    /// Zips positionally with `rhs` and keeps the left value at each position (`<*`); truncates
    /// to the shorter side.
    func seqLeft<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<Output, Failure> {
        zip(rhs).map { a, _ in a }
    }

    /// Free-function form of the instance `zip`: pairs `pa` and `pb` positionally into a publisher
    /// of tuples, truncating to the shorter side.
    static func zip<B: Sendable>(
        _ pa: Publisher<Output, Failure>,
        _ pb: Publisher<B, Failure>
    ) -> Publisher<(Output, B), Failure> {
        pa.zip(pb)
    }
}

/// Applicative apply (`<*>`) for publishers: pairs each function in `fns` with the value in
/// `values` at the same position and applies it, truncating to the shorter side. This is the
/// zippy (ZipList-style) product, not the cartesian, monad-consistent one — see the section note.
public func applyPublisher<A: Sendable, B: Sendable, E: Error>(
    _ fns: Publisher<@Sendable (A) -> B, E>,
    _ values: Publisher<A, E>
) -> Publisher<B, E> {
    fns.zip(values).map { pair in pair.0(pair.1) }
}

// MARK: - Monad

public extension Publisher {
    /// Curried, free-function form of `flatMap`: lifts `transform` into a function on publishers
    /// (Monad bind), for point-free composition.
    static func flatMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> @Sendable (Publisher<Output, Failure>) -> Publisher<T, Failure> {
        { @Sendable publisher in publisher.flatMap(transform) }
    }

    /// Flattens a publisher of publishers by one level (Monad `join`).
    func join<A: Sendable>() -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        flatMap { $0 }
    }

    /// Free-function form of `join`: flattens `nested` by one level.
    static func join<A: Sendable>(
        _ nested: Publisher<Publisher<A, Failure>, Failure>
    ) -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        nested.flatMap { $0 }
    }

    /// Left-to-right Kleisli composition (`>=>`): composes two publisher-returning functions
    /// into one.
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Output) -> Publisher<B, Failure>,
        _ g: @escaping @Sendable (B) -> Publisher<C, Failure>
    ) -> @Sendable (Output) -> Publisher<C, Failure> {
        { @Sendable a in f(a).flatMap(g) }
    }

    /// Right-to-left Kleisli composition (`<=<`): composes two publisher-returning functions,
    /// mirroring the DeferredTask/DeferredStream base.
    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Output) -> Publisher<B, Failure>,
        _ f: @escaping @Sendable (X) -> Publisher<Output, Failure>
    ) -> @Sendable (X) -> Publisher<B, Failure> {
        { @Sendable x in f(x).flatMap(g) }
    }
}

// MARK: - Alternative

public extension Publisher {
    /// Alternative `<|>` as concatenation: emits every value from `self`, then (only if `self`
    /// finished without failing) every value from `other`. A failure on either side propagates
    /// and seals the stream.
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

    /// Free-function form of `alt`: `lhs` concatenated with the lazily-evaluated `rhs`.
    static func alt(
        _ lhs: Publisher<Output, Failure>,
        _ rhs: @autoclosure () -> Publisher<Output, Failure>
    ) -> Publisher<Output, Failure> {
        lhs.alt(rhs())
    }
}
