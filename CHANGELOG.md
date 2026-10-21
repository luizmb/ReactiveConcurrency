# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `NotificationCenter.notifications(named:object:_:)`: a center's notifications as a cold
  `Publisher`, of what a transform reads off each one (`Notification` is not `Sendable`, so it never
  leaves the observer), and `notifications(named:object:)` for the bare fact. The observer is
  registered when a subscription starts and removed when it ends. Named apart from Foundation's
  Combine `publisher(for:object:)` on Apple platforms.

## [2.0.0] - 2026-10-16

FP 3 migration. Breaking: the transformer layer moves to FP 3's one-struct-per-stack model, and the
stream applicatives become lawful.

### Fixed
- Operators that feed one downstream from several tasks could drop or misorder an upstream
  failure: `merge`, `Publisher.merge(_:)`, `flatMap` (bounded and unbounded), `switchToLatest`,
  `timeout`, and the timed operators (`debounce`, `throttle`, `delay`, `collect(every:)`). Two writers
  raced on the terminal event, so a subscriber could see `.finished` instead of the failure, a value
  after the failure, or two failures. Every write now goes through a `TerminalGate`, which makes the
  first failure the last event. `TerminalOrderingTests` reproduce each race (up to 40% of runs
  under contention before the fix, none after).

### Changed
- **BREAKING**: FP dependency bumped to 3.0.0.
- **BREAKING**: transformer stacks are now generated structs (`DeferredTaskTResult<E, A>`,
  `PublisherTEither<Failure, L, A>`, `ReaderTDeferredStream<Env, A>`, …) wrapping the nested value,
  following FP 3. Lift in with `.deferredTaskT` / `.deferredStreamT` / `.publisherT` / `.readerT` /
  `.statefulT`, leave with `.rawValue`. Members: `map`, `fmap`, `pure`, `apply`, `liftA2`,
  `seqRight`, `seqLeft`, and on monad stacks `flatMap`, `bind`, `kleisli`, `kleisliBack`, plus a
  Haskell-named escape hatch (`mapExceptT`, `mapMaybeT`, `mapReaderT`, …). Operators (`<£>`, `<&>`,
  `£>`, `<£`, `<*>`, `*>`, `<*`, `>>-`, `-<<`, `>=>`, `<=<`) are defined on the stack types. Generated
  by `Scripts/GenerateTransformers.swift`.
- **BREAKING**: `<*>` / `liftA2` / `*>` / `<*` on `DeferredStream` and `Publisher` are `ap`, derived
  from the concat bind (the cartesian product in order), not zip. `<*>` now agrees with `>>-` and the
  applicative laws hold. `zip` is unchanged for positional pairing. `sequenceDeferredStream` /
  `traverseDeferredStream` / `sequencePublisher` / `traversePublisher` follow the applicative.
- **BREAKING**: `Publisher`'s FP bind (`>>-`, `-<<`, `>=>`, `<=<`, static `flatMap(_:)`, `join`,
  `kleisli`, `kleisliBack`) is ordered concat (`flatMap(maxPublishers: 1)`), not merge. The
  `flatMap(_:)` instance method keeps Combine's merge semantics.
- **BREAKING**: monad stacks derive their applicative from bind: on a short-circuit (`.failure`,
  `.left`, `nil`) the right-hand effect of `<*>` / `liftA2` no longer runs.
- `PublisherTOptional` / `PublisherTResult` positional choice is `zipAlt` (was `altPublisherOptional`
  / `altPublisherResult`); still not a lawful Alternative, no `<|>`.

### Removed
- **BREAKING**: the nested-shape API: `<£^>` / `<&^>` and every operator overload on nested values
  (`DeferredTask<Result<A, E>>`, `Reader<Env, Publisher<A, E>>`, …); the public nested-type functions
  (`mapTDeferredTaskResult`, `flatMapTPublisherEither`, `applyReaderDeferredTask`, `bindT…`,
  `kleisliT…`, `fmapT…`, …), now internal or gone.
- **BREAKING**: the monad on stacks without a lawful one (FP 3 rules): `DeferredTaskTArray`,
  `DeferredStreamTArray`, `PublisherTArray` (`ListT`), and the `StatefulT…` stacks are
  applicative-only. The empty `DeferredStreamTReader` placeholder is gone.

### Migration
- `f <£^> nested` → `f <£> nested.deferredTaskT` (or `.publisherT`, …), then `.rawValue`.
- `flatMapTDeferredTaskResult(task, fn)` → `task.deferredTaskT.flatMap { fn($0).deferredTaskT }.rawValue`.
- `nestedA >>- fn` on a nested value now resolves to the **outer** type's bind (or fails to
  compile): wrap it first. Same for `<*>`, `*>`, `<*`, `£>` and `<|>` (`DeferredTask<A?> <|> …` now
  picks FP's Optional `<|>`).
- `<*>` between streams that relied on zip: use `zip` (`Publisher.zip`, `DeferredStream.zip`).
- On Apple, FP declares Combine stacks with some of the same names (`PublisherTResult`,
  `ReaderTPublisher`, …): qualify (`ReactiveConcurrencyTransformers.PublisherTResult`) where you
  spell the type.

## [0.3.0] - 2026-07-05

### Changed
- **BREAKING**: Transformer operator returns are now `@Sendable`, tightening the concurrency
  contract across the transformer surface.

### Removed
- **BREAKING**: Removed the ZIO type in favour of the `Publisher` / `DeferredStream` surface.

## [0.2.0] - 2026-07-05

### Changed
- Internal consolidation of the operator and transformer layers.

## [0.1.0] - 2026-06-17

- Initial release: `Publisher` reactive type over Swift Concurrency, the operator module
  (`ReactiveConcurrencyOperators`), and the transformer module (`ReactiveConcurrencyTransformers`),
  built on FP and Hourglass.

[Unreleased]: https://github.com/luizmb/ReactiveConcurrency/compare/v0.3.0...main
[0.3.0]: https://github.com/luizmb/ReactiveConcurrency/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/luizmb/ReactiveConcurrency/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/luizmb/ReactiveConcurrency/releases/tag/v0.1.0
