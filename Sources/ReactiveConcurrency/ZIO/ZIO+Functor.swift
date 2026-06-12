// MARK: - Functor

public extension ZIO {
    // map :: (a -> b) -> ZIO<env, a, e> -> ZIO<env, b, e>
    func map<B: Sendable>(_ fn: @escaping @Sendable (Success) -> B) -> ZIO<Env, B, Failure> {
        ZIO<Env, B, Failure> { env in run(env).map { $0.map(fn) } }
    }

    // fmap :: (a -> b) -> ZIO<env, a, e> -> ZIO<env, b, e>
    static func fmap<B: Sendable>(
        _ fn: @escaping @Sendable (Success) -> B
    ) -> @Sendable (ZIO<Env, Success, Failure>) -> ZIO<Env, B, Failure> {
        { @Sendable zio in zio.map(fn) }
    }

    func replace<B: Sendable>(_ value: B) -> ZIO<Env, B, Failure> {
        map { _ in value }
    }

    // mapError :: (e -> e2) -> ZIO<env, a, e> -> ZIO<env, a, e2>
    func mapError<F2: Error & Sendable>(
        _ fn: @escaping @Sendable (Failure) -> F2
    ) -> ZIO<Env, Success, F2> {
        ZIO<Env, Success, F2> { env in run(env).map { $0.mapError(fn) } }
    }

    /// contramapEnvironment :: (r2 -> r) -> ZIO<r, a, e> -> ZIO<r2, a, e>
    func contramapEnvironment<GlobalEnv: Sendable>(
        _ fn: @escaping @Sendable (GlobalEnv) -> Env
    ) -> ZIO<GlobalEnv, Success, Failure> {
        ZIO<GlobalEnv, Success, Failure> { globalEnv in run(fn(globalEnv)) }
    }

    static func contramapEnvironment<GlobalEnv: Sendable>(
        _ fn: @escaping @Sendable (GlobalEnv) -> Env
    ) -> (ZIO<Env, Success, Failure>) -> ZIO<GlobalEnv, Success, Failure> {
        { $0.contramapEnvironment(fn) }
    }

    /// dimap :: (r2 -> r) -> (a -> b) -> ZIO<r, a, e> -> ZIO<r2, b, e>
    func dimap<GlobalEnv: Sendable, B: Sendable>(
        _ contramapEnv: @escaping @Sendable (GlobalEnv) -> Env,
        _ mapOutput: @escaping @Sendable (Success) -> B
    ) -> ZIO<GlobalEnv, B, Failure> {
        ZIO<GlobalEnv, B, Failure> { globalEnv in
            run(contramapEnv(globalEnv)).map { $0.map(mapOutput) }
        }
    }

    static func dimap<GlobalEnv: Sendable, B: Sendable>(
        _ contramapEnv: @escaping @Sendable (GlobalEnv) -> Env,
        _ mapOutput: @escaping @Sendable (Success) -> B
    ) -> (ZIO<Env, Success, Failure>) -> ZIO<GlobalEnv, B, Failure> {
        { $0.dimap(contramapEnv, mapOutput) }
    }
}

public func mapZIO<Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B,
    _ zio: ZIO<Env, A, E>
) -> ZIO<Env, B, E> {
    zio.map(fn)
}

public func fmapZIO<Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (A) -> B
) -> @Sendable (ZIO<Env, A, E>) -> ZIO<Env, B, E> {
    { @Sendable zio in zio.map(fn) }
}

public func contramapEnvironmentZIO<GlobalEnv: Sendable, Env: Sendable, A: Sendable, E: Error & Sendable>(
    _ fn: @escaping @Sendable (GlobalEnv) -> Env,
    _ zio: ZIO<Env, A, E>
) -> ZIO<GlobalEnv, A, E> {
    zio.contramapEnvironment(fn)
}

public func dimapZIO<GlobalEnv: Sendable, Env: Sendable, A: Sendable, B: Sendable, E: Error & Sendable>(
    _ contramapEnv: @escaping @Sendable (GlobalEnv) -> Env,
    _ mapOutput: @escaping @Sendable (A) -> B,
    _ zio: ZIO<Env, A, E>
) -> ZIO<GlobalEnv, B, E> {
    zio.dimap(contramapEnv, mapOutput)
}
