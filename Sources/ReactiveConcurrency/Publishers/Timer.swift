extension Publisher {
    /// Emits the current clock instant repeatedly at `interval` cadence.
    /// The first tick fires after one full `interval` from subscription time.
    public static func timer<C: Clock>(every interval: C.Duration, clock: C) -> Publisher<C.Instant, Never> {
        Publisher<C.Instant, Never> { continuation in
            var next = clock.now.advanced(by: interval)
            while true {
                try await clock.sleep(until: next, tolerance: nil)
                continuation.yield(clock.now)
                next = next.advanced(by: interval)
            }
        }
    }
}
