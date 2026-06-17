import Foundation
@testable import ReactiveConcurrency
import Testing

// Thread-safe box (writable via key path) for the portable, background assign.
private final class Box: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    var value: Int {
        get { lock.withLock { _value } }
        set { lock.withLock { _value = newValue } }
    }
}

@MainActor private final class MainBox {
    var value = 0
}

private func poll(_ condition: @Sendable () -> Bool) async {
    for _ in 0..<1_000 {
        if condition() { return }
        await Task.yield()
        try? await Task.sleep(nanoseconds: 2_000_000)
    }
}

@Suite struct AssignTests {
    @Test func assignWritesValuesToSendableObject() async {
        let box = Box()
        let cancellable = Publisher<Int, Never>.sequence(1...3).assign(to: \.value, on: box)
        await poll { box.value == 3 }
        #expect(box.value == 3)
        cancellable.cancel()
    }

    @MainActor
    @Test func assignOnMainWritesOnMainActorInOrder() async {
        let box = MainBox()
        let cancellable = Publisher<Int, Never>.sequence(1...5).assignOnMain(to: \.value, on: box)
        for _ in 0..<1_000 where box.value != 5 {
            try? await Task.sleep(nanoseconds: 2_000_000)
        }
        #expect(box.value == 5)
        cancellable.cancel()
    }
}

@Suite struct SequencePublisherTests {
    @Test func arrayPublisherEmitsAllElements() async {
        var out: [Int] = []
        for await v in [1, 2, 3].publisher.values { out.append(v) }
        #expect(out == [1, 2, 3])
    }

    @Test func rangePublisherEmitsAllElements() async {
        var out: [Int] = []
        for await v in (1...4).publisher.values { out.append(v) }
        #expect(out == [1, 2, 3, 4])
    }
}
