// SPDX-License-Identifier: Apache-2.0

import Foundation

public extension NotificationCenter {
    /// Every `name` notification this center posts, as a publisher of what `transform` reads off
    /// each one: from `object` only, or from anyone when it is `nil`.
    ///
    /// `Notification` is not `Sendable` (its `object` and `userInfo` are `Any`), so it never leaves
    /// the observer: `transform` runs there, synchronously, as the notification is posted, and only
    /// its `Sendable` answer is published.
    ///
    /// Cold, like every publisher here: the observer is registered when a subscription starts and
    /// removed when it ends, so each subscription is its own observer and an unsubscribed one costs
    /// nothing. A notification posted before the subscription has started is not seen.
    ///
    /// Named apart from Foundation's Combine `publisher(for:object:)`, which shares the receiver
    /// on Apple platforms and would make every call ambiguous.
    ///
    /// ```swift
    /// let routes = NotificationCenter.default
    ///     .notifications(named: AVAudioSession.routeChangeNotification) { _ in AVAudioSession.sharedInstance().currentRoute.domain }
    /// ```
    func notifications<Output: Sendable>(
        named name: Notification.Name,
        object: (any AnyObject & Sendable)? = nil,
        _ transform: @escaping @Sendable (Notification) -> Output
    ) -> ReactiveConcurrency.Publisher<Output, Never> {
        ReactiveConcurrency.Publisher { continuation in
            let observer = self.addObserver(forName: name, object: object, queue: nil) { continuation.yield(transform($0)) }
            await continuation.suspendUntilCancelled()
            self.removeObserver(observer)
        }
    }

    /// Every `name` notification this center posts, as the bare fact that it was posted: from
    /// `object` only, or from anyone when it is `nil`. See ``notifications(named:object:_:)``.
    ///
    /// ```swift
    /// let activations = NotificationCenter.default
    ///     .notifications(named: UIApplication.didBecomeActiveNotification)
    ///     .map { AppPhase.active }
    /// ```
    func notifications(
        named name: Notification.Name,
        object: (any AnyObject & Sendable)? = nil
    ) -> ReactiveConcurrency.Publisher<Void, Never> {
        notifications(named: name, object: object) { _ in () }
    }
}
