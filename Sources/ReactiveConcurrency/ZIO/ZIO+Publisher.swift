// ZIO -> Publisher bridge.
//
// ZIO<Env, Success, Failure> ≅ (Env) -> DeferredTask<Result<Success, Failure>>. Providing an
// environment yields a single deferred Result, which erases to a cold single-element Publisher
// that emits `Success` then finishes, or fails with `Failure`. Re-runs the ZIO per subscription.

public extension ZIO {
    func eraseToPublisher(environment: Env) -> Publisher<Success, Failure> {
        provide(environment).eraseToThrowingPublisher()
    }
}

public extension ZIO where Env == Void {
    func eraseToPublisher() -> Publisher<Success, Failure> {
        provide(()).eraseToThrowingPublisher()
    }
}
