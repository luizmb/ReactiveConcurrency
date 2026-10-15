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

// MARK: - Applicative

//
// `apply` / `liftA2` / `seqLeft` / `seqRight` are `ap`, derived from the monad: the cartesian
// product in order (for each left value, every right value), built on the ordered, concat bind
// (`flatMap(maxPublishers: 1)`). So `<*>` agrees with `>>-` and the applicative laws hold. The right
// publisher is subscribed once per left value. For positional, ZipList-style pairing use `zip`.

public extension Publisher {
    /// Lifts a single value into a one-element publisher (Applicative `pure`).
    static func pure(_ value: Output) -> Publisher<Output, Failure> {
        just(value)
    }

    /// Sequences with `rhs`, keeping `rhs`'s values (`*>`): for each value of `self`, every value of
    /// `rhs`.
    func seqRight<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<B, Failure> {
        liftA2Publisher { (_: Output, right: B) in right }(self, rhs)
    }

    /// Sequences with `rhs`, keeping `self`'s values (`<*`): each value of `self` once per value of
    /// `rhs`.
    func seqLeft<B: Sendable>(_ rhs: Publisher<B, Failure>) -> Publisher<Output, Failure> {
        liftA2Publisher { (left: Output, _: B) in left }(self, rhs)
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

/// Applicative apply (`<*>`) for publishers: for each function in `fns`, in order, every value of
/// `values` (`ap`, derived from the concat bind). See the section note.
public func applyPublisher<A: Sendable, B: Sendable, E: Error>(
    _ fns: Publisher<@Sendable (A) -> B, E>,
    _ values: Publisher<A, E>
) -> Publisher<B, E> {
    fns.flatMap(maxPublishers: 1) { fn in values.map(fn) }
}

/// Combines two publishers with a binary function: for each value of `pa`, in order, every value of
/// `pb` (`liftA2`, derived from the concat bind). For positional pairing use `zip`.
public func liftA2Publisher<A: Sendable, B: Sendable, C: Sendable, E: Error>(
    _ fn: @escaping @Sendable (A, B) -> C
) -> @Sendable (Publisher<A, E>, Publisher<B, E>) -> Publisher<C, E> {
    { @Sendable pa, pb in
        pa.flatMap(maxPublishers: 1) { a in pb.map { b in fn(a, b) } }
    }
}

// MARK: - Monad

//
// The FP algebra (`flatMap(_:)` as a free function, `join`, `kleisli`, `>>-`) binds with ordered
// concat (`flatMap(maxPublishers: 1)`): each inner publisher completes before the next starts, so
// the result order is deterministic and the monad laws hold. The instance method `flatMap(_:)`
// keeps Combine's merge semantics (unbounded concurrency, arrival order).

public extension Publisher {
    /// Curried, free-function form of `flatMap`: lifts `transform` into a function on publishers
    /// (Monad bind), for point-free composition.
    static func flatMap<T: Sendable>(
        _ transform: @escaping @Sendable (Output) -> Publisher<T, Failure>
    ) -> @Sendable (Publisher<Output, Failure>) -> Publisher<T, Failure> {
        { @Sendable publisher in publisher.flatMap(maxPublishers: 1, transform) }
    }

    /// Flattens a publisher of publishers by one level (Monad `join`).
    func join<A: Sendable>() -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        flatMap(maxPublishers: 1) { $0 }
    }

    /// Free-function form of `join`: flattens `nested` by one level.
    static func join<A: Sendable>(
        _ nested: Publisher<Publisher<A, Failure>, Failure>
    ) -> Publisher<A, Failure> where Output == Publisher<A, Failure> {
        nested.flatMap(maxPublishers: 1) { $0 }
    }

    /// Left-to-right Kleisli composition (`>=>`): composes two publisher-returning functions
    /// into one.
    static func kleisli<B: Sendable, C: Sendable>(
        _ f: @escaping @Sendable (Output) -> Publisher<B, Failure>,
        _ g: @escaping @Sendable (B) -> Publisher<C, Failure>
    ) -> @Sendable (Output) -> Publisher<C, Failure> {
        { @Sendable a in f(a).flatMap(maxPublishers: 1, g) }
    }

    /// Right-to-left Kleisli composition (`<=<`): composes two publisher-returning functions,
    /// mirroring the DeferredTask/DeferredStream base.
    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Output) -> Publisher<B, Failure>,
        _ f: @escaping @Sendable (X) -> Publisher<Output, Failure>
    ) -> @Sendable (X) -> Publisher<B, Failure> {
        { @Sendable x in f(x).flatMap(maxPublishers: 1, g) }
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
