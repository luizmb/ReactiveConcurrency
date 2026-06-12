// MARK: - Monad (Input fixed, sequences ZIO results)

public extension ZIOKleisli {
    static func pure(_ value: Success) -> ZIOKleisli<Input, Env, Success, Failure> {
        ZIOKleisli { _ in .pure(value) }
    }

    func flatMap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> ZIOKleisli<Input, Env, B, Failure>
    ) -> ZIOKleisli<Input, Env, B, Failure> {
        ZIOKleisli<Input, Env, B, Failure> { input in
            run(input).flatMap { a in fn(a).run(input) }
        }
    }

    static func bind<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> ZIOKleisli<Input, Env, B, Failure>
    ) -> @Sendable (ZIOKleisli<Input, Env, Success, Failure>) -> ZIOKleisli<Input, Env, B, Failure> {
        { @Sendable k in k.flatMap(fn) }
    }

    static func join<A: Sendable>(
        _ nested: ZIOKleisli<Input, Env, ZIOKleisli<Input, Env, A, Failure>, Failure>
    ) -> ZIOKleisli<Input, Env, A, Failure> where Success == ZIOKleisli<Input, Env, A, Failure> {
        nested.flatMap { $0 }
    }

    func flatMapError<F2: Error & Sendable>(
        _ fn: @escaping @Sendable (Failure) -> ZIOKleisli<Input, Env, Success, F2>
    ) -> ZIOKleisli<Input, Env, Success, F2> {
        ZIOKleisli<Input, Env, Success, F2> { input in
            run(input).flatMapError { e in fn(e).run(input) }
        }
    }

    func void() -> ZIOKleisli<Input, Env, Void, Failure> {
        map { _ in () }
    }
}

// MARK: - Kleisli category composition (changes Input type)

public extension ZIOKleisli {
    func andThen<B: Sendable>(
        _ next: ZIOKleisli<Success, Env, B, Failure>
    ) -> ZIOKleisli<Input, Env, B, Failure> {
        ZIOKleisli<Input, Env, B, Failure> { input in
            run(input).flatMap { a in next.run(a) }
        }
    }

    func compose<PrevInput: Sendable>(
        _ prev: ZIOKleisli<PrevInput, Env, Input, Failure>
    ) -> ZIOKleisli<PrevInput, Env, Success, Failure> {
        prev.andThen(self)
    }

    static func kleisli<I: Sendable, B: Sendable>(
        _ f: ZIOKleisli<I, Env, Success, Failure>,
        _ g: ZIOKleisli<Success, Env, B, Failure>
    ) -> ZIOKleisli<I, Env, B, Failure> {
        f.andThen(g)
    }
}

public func flatMapZIOKleisli<I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ k: ZIOKleisli<I, Env, A, E>,
    _ fn: @escaping @Sendable (A) -> ZIOKleisli<I, Env, B, E>
) -> ZIOKleisli<I, Env, B, E> {
    k.flatMap(fn)
}

public func bindZIOKleisli<I: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> ZIOKleisli<I, Env, B, E>
) -> @Sendable (ZIOKleisli<I, Env, A, E>) -> ZIOKleisli<I, Env, B, E> {
    { @Sendable k in k.flatMap(fn) }
}
