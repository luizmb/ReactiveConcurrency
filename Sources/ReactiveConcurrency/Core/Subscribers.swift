// SPDX-License-Identifier: Apache-2.0

public enum Subscribers {}

public extension Subscribers {
    enum Completion<Failure: Error>: Sendable where Failure: Sendable {
        case finished
        case failure(Failure)
    }
}
