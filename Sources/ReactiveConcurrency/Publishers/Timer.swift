// SPDX-License-Identifier: Apache-2.0

import Hourglass

public extension Publisher {
    /// Emits the current clock instant repeatedly at `interval` cadence.
    /// The first tick fires after one full `interval` from subscription time.
    /// Backed by Hourglass's `timerSequence`; restarts on each subscription (cold).
    static func timer<C: Clock & Sendable>(
        every interval: C.Instant.Duration,
        clock: C
    ) -> Publisher<C.Instant, Never> {
        DeferredStream { timerSequence(every: interval, clock: clock) }.eraseToPublisher()
    }
}
