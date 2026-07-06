// SPDX-License-Identifier: Apache-2.0

import Foundation

// Drop-in replacement for Synchronization.Mutex that works on iOS 16 / macOS 13.
// withLock takes an inout Value parameter, matching Mutex's API exactly.
final class Locked<Value: Sendable>: @unchecked Sendable {
    private let _lock = NSLock()
    private var _value: Value

    init(_ value: Value) { _value = value }

    @discardableResult
    func withLock<R>(_ body: (inout Value) -> R) -> R {
        _lock.lock()
        defer { _lock.unlock() }
        return body(&_value)
    }
}
