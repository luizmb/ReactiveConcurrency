// SPDX-License-Identifier: Apache-2.0

@testable import ReactiveConcurrency
import Testing

// Serialized: Diagnostics is process-global, so these must not interleave.
@Suite(.serialized, .timeLimit(.minutes(1))) struct DiagnosticsTests {
    @Test func warnsOnSendAfterCompletion() async {
        let captured = Collector<String>()
        Diagnostics.setHandler { captured.append($0) }
        defer { Diagnostics.resetHandler() }

        let subject = PassthroughSubject<Int, Never>()
        subject.send(completion: .finished)
        subject.send(1)

        #expect(captured.values.count == 1)
        #expect(captured.values.first?.contains("already completed") == true)
    }

    @Test func warnsOnCompleteAfterCompletion() async {
        let captured = Collector<String>()
        Diagnostics.setHandler { captured.append($0) }
        defer { Diagnostics.resetHandler() }

        let subject = PassthroughSubject<Int, Never>()
        subject.send(completion: .finished)
        subject.send(completion: .finished)

        #expect(captured.values.contains { $0.contains("ignored") })
    }

    @Test func normalUsageDoesNotWarn() async {
        let captured = Collector<String>()
        Diagnostics.setHandler { captured.append($0) }
        defer { Diagnostics.resetHandler() }

        let subject = PassthroughSubject<Int, Never>()
        subject.send(1); subject.send(2)
        subject.send(completion: .finished)

        #expect(captured.values.isEmpty)
    }

    @Test func disabledSuppressesWarnings() async {
        let captured = Collector<String>()
        Diagnostics.setHandler { captured.append($0) }
        Diagnostics.isEnabled = false
        defer { Diagnostics.resetHandler(); Diagnostics.isEnabled = true }

        let subject = PassthroughSubject<Int, Never>()
        subject.send(completion: .finished)
        subject.send(1)

        #expect(captured.values.isEmpty)
    }
}
