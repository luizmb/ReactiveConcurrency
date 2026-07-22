# Release Process

ReactiveConcurrency is distributed **exclusively via Swift Package Manager** — a release is a
signed-off git tag plus a GitHub release with auto-generated notes. No binaries, frameworks, or
XCFrameworks are produced or attached; consumers always build from source through SPM.

## Quick Reference

```bash
# 1. Create a release candidate branch
#    Preferred: Actions → Create Release Candidate → Run workflow → enter version
#    Fallback:  git checkout -b release/1.0.0 && git push origin release/1.0.0

# 2. CI automatically builds and tests the branch on every platform
# → Watch the Actions tab for the RC build to go green

# 3. When ready to promote, tag from the RC branch
git checkout release/1.0.0
git tag v1.0.0
git push origin v1.0.0

# 4. CI validates the tag and publishes the GitHub release (notes auto-generated from commits)
```

## Overview

The release process has two stages:

### Stage 1: Release Candidate (RC)
- Create a `release/X.Y.Z` branch.
- CI builds and tests the branch on macOS, Linux, Windows, and Android.
- Iterate and fix issues on the branch without any tag manipulation.

### Stage 2: Release Promotion
- Create a version tag `vX.Y.Z` from the RC branch.
- The tag triggers the promotion workflow: it validates the tag and publishes a GitHub release
  with auto-generated notes (the commit log since the previous tag).

## Release Workflow

### Stage 1: Create the Release Candidate branch

Go to **Actions → Create Release Candidate → Run workflow** and enter the version (e.g. `1.0.0`).
The workflow creates and pushes the `release/1.0.0` branch. Or create it manually:

```bash
git checkout -b release/1.0.0
git push origin release/1.0.0
```

**What happens automatically** (`release.yml`, on `release/*` branches):
- Runs lint, build, the full test suite, and Periphery on macOS.
- Runs build + tests on Linux, Windows, and Android.
- Posts a status notification.

**On the RC branch you can** fix bugs, update version references, and polish docs — push commits
normally (`git push origin release/1.0.0`); each push re-runs the RC checks.

### Stage 2: Promote the RC to a Release

When the RC branch is green:

```bash
git checkout release/1.0.0
git tag v1.0.0
git push origin v1.0.0
```

**What happens automatically** (`release.yml`, on `v*` tags):
- Verifies the tag format (`vX.Y.Z`) and that a corresponding `release/X.Y.Z` branch exists.
- Creates a GitHub release titled `Release vX.Y.Z` with notes generated from the commit log since
  the previous tag, plus an SPM installation snippet.
- Posts a completion notification.

**If the promotion fails**, delete the tag, fix on the RC branch, and re-tag:

```bash
git tag -d v1.0.0
git push origin :refs/tags/v1.0.0
git push origin release/1.0.0   # push fixes to the RC branch
git tag v1.0.0
git push origin v1.0.0
```

## Consuming a Release

Add the package with Swift Package Manager and depend on the products you need:

```swift
dependencies: [
    .package(url: "https://github.com/luizmb/ReactiveConcurrency.git", from: "1.1.0"),
],
targets: [
    .target(name: "MyApp", dependencies: [
        .product(name: "ReactiveConcurrency", package: "ReactiveConcurrency"),
        // optional operator syntax:
        .product(name: "ReactiveConcurrencyOperators", package: "ReactiveConcurrency"),
        // optional monad-transformer surface:
        .product(name: "ReactiveConcurrencyTransformers", package: "ReactiveConcurrency"),
    ]),
]
```

In Xcode: **File → Add Package Dependencies…**, enter the repository URL, and pick a version rule.

## Supported Platforms & Toolchain

| | |
|---|---|
| Swift tools version | 6.3 |
| Xcode (CI) | 26.5 |
| Apple platforms | macOS 13+, iOS 16+, tvOS 16+, watchOS 9+, visionOS 1+ |
| Other platforms | Linux, Windows, Android (built & tested in CI) |

`swift-docc-plugin` is only pulled in on non-Windows hosts (its command plugin doesn't build on
Windows and is only needed to generate documentation on macOS) — see the `#if !os(Windows)` guard in
`Package.swift`.

## Troubleshooting

### RC build fails
Review the failed jobs in the Actions run for the `release/X.Y.Z` branch, push fixes to that branch,
and let CI re-run. No tags are involved at this stage.

### Promotion fails after pushing the tag
The tag exists but no release was created. Recover by deleting the tag, fixing on the RC branch, and
re-tagging (see the snippet in Stage 2). Common causes: the tag doesn't match `vX.Y.Z`, or it was
pushed from a commit not on the `release/X.Y.Z` branch.

### Release not appearing after tag push
Confirm the tag matches `v[0-9]+.[0-9]+.[0-9]+` and that the `create-release` job succeeded in the
Actions run for the tag.

## CI/CD Files

- `.github/workflows/ci.yml` — build/test/lint on every PR and push to `main` (5-platform matrix,
  serial tests, per-job timeouts).
- `.github/workflows/create-rc.yml` — creates a `release/X.Y.Z` branch from a manual dispatch.
- `.github/workflows/release.yml` — RC checks on `release/*` branches; release creation on `v*` tags.
- `.github/workflows/promote-rc.yml` — helper to promote an RC branch to a tag.
- `.github/workflows/docs.yml` — builds the DocC catalog and deploys it to GitHub Pages
  (https://ios.lu/ReactiveConcurrency/).

## References

- [Swift Package Manager](https://www.swift.org/documentation/package-manager/)
- [Semantic Versioning](https://semver.org/)
