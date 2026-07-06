// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Opt-in runtime diagnostics for catching common misuse. Enabled by default in DEBUG builds and
/// off in release, with a configurable handler (default: stderr).
///
/// Note on reentrancy: unlike RxSwift, subjects here deliver asynchronously via `AsyncStream` —
/// `send` only buffers into each subscriber's continuation and never invokes subscriber code
/// synchronously, so synchronous reentrancy (a subscriber re-entering `send` mid-delivery) cannot
/// occur and needs no detector. These diagnostics focus on lifecycle misuse instead.
public enum Diagnostics {
    private struct Config: Sendable {
        var isEnabled: Bool
        var handler: @Sendable (String) -> Void
    }

    private static let config = Locked(Config(isEnabled: defaultEnabled, handler: defaultHandler))

    private static var defaultEnabled: Bool {
        #if DEBUG
            true
        #else
            false
        #endif
    }

    private static let defaultHandler: @Sendable (String) -> Void = { message in
        FileHandle.standardError.write(Data((message + "\n").utf8))
    }

    /// Whether diagnostics are emitted. Defaults to `true` in DEBUG, `false` in release.
    public static var isEnabled: Bool {
        get { config.withLock { $0.isEnabled } }
        set { config.withLock { $0.isEnabled = newValue } }
    }

    /// Route diagnostic messages somewhere other than stderr (e.g. a logger or a test collector).
    public static func setHandler(_ handler: @escaping @Sendable (String) -> Void) {
        config.withLock { $0.handler = handler }
    }

    /// Restore the default stderr handler.
    public static func resetHandler() {
        config.withLock { $0.handler = defaultHandler }
    }

    // Evaluates and emits `message` only when enabled — zero cost (a single lock read) otherwise.
    static func warn(_ message: @autoclosure () -> String) {
        let snapshot = config.withLock { ($0.isEnabled, $0.handler) }
        guard snapshot.0 else { return }
        snapshot.1("[ReactiveConcurrency] " + message())
    }
}
