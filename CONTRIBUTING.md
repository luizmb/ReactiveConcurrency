# Contributing to ReactiveConcurrency

Thank you for your interest in contributing to ReactiveConcurrency!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/ReactiveConcurrency.git`
3. Install tools: `brew install mint && mint bootstrap`
4. Build: `swift build 2>&1 | xcsift`
5. Test: `swift test 2>&1 | xcsift`

## Branch Naming

| Pattern | Purpose |
|---------|---------|
| `feature/name` | New feature |
| `bugfix/name` | Bug fix |

Never push directly to `main` — always open a PR.

## Before Submitting

- [ ] `mint run swiftformat --lint .` passes (run `--write` to auto-fix)
- [ ] `mint run swiftlint lint --strict` passes
- [ ] `swift test` passes on macOS
- [ ] `mint run periphery scan` passes
- [ ] New public APIs have DocC documentation (inline `///` + a `.docc` topic/article where relevant)
- [ ] CHANGELOG.md updated under `## [Unreleased]`
- [ ] Every new `.swift` file starts with `// SPDX-License-Identifier: Apache-2.0`

## Code Style

This library follows strict, Haskell-inspired functional programming. See the README and
`CLAUDE.md` for detailed guidance:

- Pure functions — no side effects; inject all ambient state (`Date`, `UUID`, `Calendar`,
  `Locale`, `TimeZone`, storage) as parameters
- `Result<Success, SpecificFailure>` over `throws`; `DeferredTask` over `async/await`
- Tacit / point-free composition with the FP operators (`>>>`, `|>`, `<£>`, `>>-`)
- No force unwraps, no crash functions (`fatalError`, `preconditionFailure`, …)
- `Sendable`-first; no singletons or global mutable state

## Transformer stacks

The monad-transformer stacks follow FP 3's model: each stack is its own struct named
`OuterTInner` (`DeferredTaskTResult`, `ReaderTPublisher`, …) wrapping the whole nested value, with
`map` / `pure` / `apply` / `liftA2` / `seqRight` / `seqLeft` and, on lawful monads, `flatMap` /
`bind` / `kleisli` / `kleisliBack`. The operators live in `ReactiveConcurrencyOperators` and are
defined on the stack type only: never add operator overloads on nested shapes
(`DeferredTask<Result<A, E>>`).

- **Generated code.** All stacks come from `swift Scripts/GenerateTransformers.swift`, into
  `Sources/ReactiveConcurrencyTransformers/Generated/` (structs) and
  `Sources/ReactiveConcurrencyOperators/Generated/` (operators). Never hand-edit generated files:
  change the generator, or add a `Stack(.outer, .inner, kind)` line to its `inventory`, rerun it,
  and commit script and output together.
- **Implementation.** The structs delegate `map` / `flatMap` (and, on applicative-only stacks,
  `apply` / `liftA2`) to the internal nested-type functions in `ReactiveConcurrencyTransformers`
  (`mapTDeferredTaskResult`, `flatMapTPublisherEither`, …). Keep those `internal`.
- **Laws decide the kind.** A stack is a monad only if the monad laws hold (Haskell is the source
  of truth): `Array` inside an effect (`ListT`), `Validation`, and `Stateful` outside an effect are
  applicative-only. A monad stack's applicative is derived from its bind (`ap`), so `<*>` always
  agrees with `>>-`; the generator does this, don't write a separate `apply`.
- **Hand-written extras** (like `alt` / `race` on `DeferredTaskTOptional`) go in non-generated
  files next to the stack's internal functions, with their operators in a non-generated file of
  the operators module.

## Pull Request Process

1. Open a PR against `main` using the PR template (What / Why / How / Tests).
2. All CI checks (format, lint, build, test, periphery, cross-platform) must pass.
3. A maintainer reviews and merges. Breaking changes are documented in `CHANGELOG.md`.

## License

By contributing, you agree that your contributions are licensed under the Apache License 2.0.
