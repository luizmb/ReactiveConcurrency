// ZIOKleisli<Input, Env, Success, Failure>
// = (Input) -> ZIO<Env, Success, Failure>
// — a first-class Kleisli arrow in the ZIO monad.

/// A first-class Kleisli arrow in the ``ZIO`` monad.
///
/// `ZIOKleisli<Input, Env, Success, Failure>` is a named wrapper around the function type
/// `(Input) -> ZIO<Env, Success, Failure>`. It is the "one up" level of composition:
/// while `>=>` on plain functions produces another plain function, composing `ZIOKleisli`
/// values produces another `ZIOKleisli` — a named, first-class type that can be stored,
/// inspected, and further composed.
///
/// - SeeAlso: ``ZIO``, ``DeferredTask``
public struct ZIOKleisli<Input: Sendable, Env: Sendable, Success: Sendable, Failure: Error & Sendable>: Sendable {
    public let run: @Sendable (Input) -> ZIO<Env, Success, Failure>

    public init(_ run: @escaping @Sendable (Input) -> ZIO<Env, Success, Failure>) {
        self.run = run
    }

    public func callAsFunction(_ input: Input) -> ZIO<Env, Success, Failure> {
        run(input)
    }

    /// Lift a ZIO into a ZIOKleisli that ignores its input.
    public static func lift(_ zio: ZIO<Env, Success, Failure>) -> ZIOKleisli {
        ZIOKleisli { _ in zio }
    }
}
