import CoreFPOperators
@testable import ReactiveConcurrency
import ReactiveConcurrencyOperators
import Testing

// Reverse Kleisli (kleisliBack / <=<) for DeferredTask and DeferredStream — parity with the
// forward kleisli (>=>). `g <=< f` runs f first, then g, so it equals `f >=> g`.

@Suite struct ReverseKleisliTests {
    @Test func deferredTaskKleisliBackComposes() async {
        let f: @Sendable (Int) -> DeferredTask<Int> = { x in DeferredTask { x + 1 } }
        let g: @Sendable (Int) -> DeferredTask<String> = { x in DeferredTask { "v\(x)" } }

        let composed = DeferredTask<Int>.kleisliBack(g, f)
        #expect(await composed(2).run() == "v3")
    }

    @Test func deferredTaskReverseOperatorMatchesForward() async {
        let f: @Sendable (Int) -> DeferredTask<Int> = { x in DeferredTask { x + 1 } }
        let g: @Sendable (Int) -> DeferredTask<String> = { x in DeferredTask { "v\(x)" } }

        let back = await (g <=< f)(2).run()
        let forward = await (f >=> g)(2).run()
        #expect(back == "v3")
        #expect(back == forward)
    }

    @Test func deferredStreamKleisliBackComposes() async {
        let f: @Sendable (Int) -> DeferredStream<Int> = { x in .pure(x + 1) }
        let g: @Sendable (Int) -> DeferredStream<String> = { x in .pure("v\(x)") }

        var out: [String] = []
        for await value in DeferredStream<Int>.kleisliBack(g, f)(2) { out.append(value) }
        #expect(out == ["v3"])
    }

    @Test func deferredStreamReverseOperatorMatchesForward() async {
        let f: @Sendable (Int) -> DeferredStream<Int> = { x in .pure(x + 1) }
        let g: @Sendable (Int) -> DeferredStream<String> = { x in .pure("v\(x)") }

        var back: [String] = []
        for await value in (g <=< f)(2) { back.append(value) }
        var forward: [String] = []
        for await value in (f >=> g)(2) { forward.append(value) }
        #expect(back == ["v3"])
        #expect(back == forward)
    }
}
