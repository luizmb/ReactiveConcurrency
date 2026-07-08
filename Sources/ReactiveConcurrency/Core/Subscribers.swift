// SPDX-License-Identifier: Apache-2.0

/// Namespace for subscriber-related types.
public enum Subscribers {}

public extension Subscribers {
    /// How a publisher terminated: either normally, or with a failure.
    enum Completion<Failure: Error>: Sendable where Failure: Sendable {
        /// The publisher finished normally, emitting no further values.
        case finished
        /// The publisher terminated with a failure.
        case failure(Failure)
    }
}
