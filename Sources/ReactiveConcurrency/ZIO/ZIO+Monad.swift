// MARK: - Monad

public extension ZIO {
    // flatMap :: (a -> ZIO<env, b, e>) -> ZIO<env, a, e> -> ZIO<env, b, e>
    func flatMap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> ZIO<Env, B, Failure>
    ) -> ZIO<Env, B, Failure> {
        ZIO<Env, B, Failure> { env in
            run(env).flatMap { result in
                switch result {
                case let .success(a): fn(a).run(env)
                case let .failure(e): .pure(.failure(e))
                }
            }
        }
    }

    static func bind<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> ZIO<Env, B, Failure>
    ) -> @Sendable (ZIO<Env, Success, Failure>) -> ZIO<Env, B, Failure> {
        { @Sendable zio in zio.flatMap(fn) }
    }

    static func join<A: Sendable>(
        _ nested: ZIO<Env, ZIO<Env, A, Failure>, Failure>
    ) -> ZIO<Env, A, Failure> where Success == ZIO<Env, A, Failure> {
        nested.flatMap { $0 }
    }

    static func kleisli<X: Sendable, B: Sendable>(
        _ f: @escaping @Sendable (X) -> ZIO<Env, Success, Failure>,
        _ g: @escaping @Sendable (Success) -> ZIO<Env, B, Failure>
    ) -> @Sendable (X) -> ZIO<Env, B, Failure> {
        { @Sendable x in f(x).flatMap(g) }
    }

    static func kleisliBack<X: Sendable, B: Sendable>(
        _ g: @escaping @Sendable (Success) -> ZIO<Env, B, Failure>,
        _ f: @escaping @Sendable (X) -> ZIO<Env, Success, Failure>
    ) -> @Sendable (X) -> ZIO<Env, B, Failure> {
        kleisli(f, g)
    }

    // flatMapError :: (e -> ZIO<env, a, e2>) -> ZIO<env, a, e> -> ZIO<env, a, e2>
    func flatMapError<F2: Error & Sendable>(
        _ fn: @escaping @Sendable (Failure) -> ZIO<Env, Success, F2>
    ) -> ZIO<Env, Success, F2> {
        ZIO<Env, Success, F2> { env in
            run(env).flatMap { result in
                switch result {
                case let .success(a): .pure(.success(a))
                case let .failure(e): fn(e).run(env)
                }
            }
        }
    }

    func void() -> ZIO<Env, Void, Failure> {
        map { _ in () }
    }

    static var ask: ZIO<Env, Env, Failure> {
        ZIO<Env, Env, Failure> { env in .pure(.success(env)) }
    }

    static func asks<B: Sendable>(
        _ fn: @escaping @Sendable (Env) -> B
    ) -> ZIO<Env, B, Failure> {
        ZIO<Env, B, Failure> { env in .pure(.success(fn(env))) }
    }

    func local(_ fn: @escaping @Sendable (Env) -> Env) -> ZIO<Env, Success, Failure> {
        ZIO { env in run(fn(env)) }
    }
}

public func flatMapZIO<Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ zio: ZIO<Env, A, E>,
    _ fn: @escaping @Sendable (A) -> ZIO<Env, B, E>
) -> ZIO<Env, B, E> {
    zio.flatMap(fn)
}

public func bindZIO<Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> ZIO<Env, B, E>
) -> @Sendable (ZIO<Env, A, E>) -> ZIO<Env, B, E> {
    { @Sendable zio in zio.flatMap(fn) }
}
