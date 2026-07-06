# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
