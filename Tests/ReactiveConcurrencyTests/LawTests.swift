// SPDX-License-Identifier: Apache-2.0

// Algebraic law tests for the base algebra (DeferredTask, DeferredStream, Publisher), the
// WriterT stack (locks in the B3 log-combining fix), the concat-alt monoid (with the new
// DeferredStream.empty identity), and Validation error accumulation.
//
// Where an instance is intentionally NOT lawful — the zippy (ZipList) stream applicative — the
// test asserts the actual truncating behaviour so the semantics are pinned rather than silently
// drifting toward a claim of lawfulness.

import DataStructure
@testable import ReactiveConcurrency
import ReactiveConcurrencyTransformers
import Testing

// MARK: - Collection helpers

private func run<A: Sendable>(_ task: DeferredTask<A>) async -> A {
    await task.run()
}

private func collect<A: Sendable>(_ stream: DeferredStream<A>) async -> [A] {
    var out: [A] = []
    for await element in stream { out.append(element) }
    return out
}

private func collect<A: Sendable, F: Error>(_ publisher: Publisher<A, F>) async -> [A] {
    var out: [A] = []
    for await result in publisher._stream {
        if case let .success(value) = result { out.append(value) }
    }
    return out
}

private let inc: @Sendable (Int) -> Int = { $0 + 1 }
private let dbl: @Sendable (Int) -> Int = { $0 * 2 }

// MARK: - DeferredTask (fully lawful Functor / Applicative / Monad)

@Suite(.timeLimit(.minutes(1))) struct DeferredTaskLawTests {
    @Test func functorIdentity() async {
        let t = DeferredTask { 5 }
        #expect(await run(t.map { $0 }) == run(t))
    }

    @Test func functorComposition() async {
        let t = DeferredTask { 5 }
        #expect(await run(t.map(inc).map(dbl)) == run(t.map { dbl(inc($0)) }))
    }

    @Test func applicativeIdentity() async {
        let v = DeferredTask { 7 }
        let idFn = DeferredTask<@Sendable (Int) -> Int> { { $0 } }
        #expect(await run(applyDeferredTask(idFn, v)) == run(v))
    }

    @Test func applicativeHomomorphism() async {
        let x = 3
        let lhs = applyDeferredTask(DeferredTask<@Sendable (Int) -> Int> { inc }, DeferredTask { x })
        let rhs = DeferredTask { inc(x) }
        #expect(await run(lhs) == run(rhs))
    }

    @Test func monadLeftIdentity() async {
        let a = 5
        let f: @Sendable (Int) -> DeferredTask<Int> = { n in DeferredTask { n * 10 } }
        #expect(await run(DeferredTask.pure(a).flatMap(f)) == run(f(a)))
    }

    @Test func monadRightIdentity() async {
        let m = DeferredTask { 9 }
        #expect(await run(m.flatMap { DeferredTask.pure($0) }) == run(m))
    }

    @Test func monadAssociativity() async {
        let m = DeferredTask { 2 }
        let f: @Sendable (Int) -> DeferredTask<Int> = { n in DeferredTask { n + 1 } }
        let g: @Sendable (Int) -> DeferredTask<Int> = { n in DeferredTask { n * 3 } }
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await run(lhs) == run(rhs))
    }

    // ap-consistency: for DeferredTask, apply == flatMap+map (the property B2 found broken for streams).
    @Test func apEqualsBind() async {
        let fns = DeferredTask<@Sendable (Int) -> Int> { inc }
        let vals = DeferredTask { 10 }
        let viaAp = applyDeferredTask(fns, vals)
        let viaBind = fns.flatMap { f in vals.map(f) }
        #expect(await run(viaAp) == run(viaBind))
    }
}

// MARK: - DeferredStream (lawful Functor / Monad; zippy Semigroupal; concat-alt monoid)

@Suite(.timeLimit(.minutes(1))) struct DeferredStreamLawTests {
    private func from(_ xs: [Int]) -> DeferredStream<Int> {
        DeferredStream<Int> { AsyncStream { c in
            for x in xs { c.yield(x) }
            c.finish()
        } }
    }

    @Test func functorIdentity() async {
        let s = from([1, 2, 3])
        #expect(await collect(s.map { $0 }) == collect(s))
    }

    @Test func functorComposition() async {
        let s = from([1, 2, 3])
        #expect(await collect(s.map(inc).map(dbl)) == collect(s.map { dbl(inc($0)) }))
    }

    @Test func monadLeftIdentity() async {
        let a = 5
        let f: @Sendable (Int) -> DeferredStream<Int> = { n in DeferredStream<Int> { AsyncStream { c in
            c.yield(n); c.yield(n + 1); c.finish()
        } } }
        #expect(await collect(DeferredStream.pure(a).flatMap(f)) == collect(f(a)))
    }

    @Test func monadRightIdentity() async {
        let m = from([1, 2, 3])
        #expect(await collect(m.flatMap { DeferredStream.pure($0) }) == collect(m))
    }

    @Test func monadAssociativity() async {
        let m = from([1, 2])
        let f: @Sendable (Int) -> DeferredStream<Int> = { n in DeferredStream<Int> { AsyncStream { c in
            c.yield(n); c.yield(n * 10); c.finish()
        } } }
        let g: @Sendable (Int) -> DeferredStream<Int> = { n in DeferredStream<Int> { AsyncStream { c in
            c.yield(n + 100); c.finish()
        } } }
        let lhs = m.flatMap(f).flatMap(g)
        let rhs = m.flatMap { a in f(a).flatMap(g) }
        #expect(await collect(lhs) == collect(rhs))
    }

    // NOT lawful: the zippy applicative truncates at the shorter side, so the Applicative identity
    // law fails for |v| > 1. Pin the actual behaviour (this is what B1 documented).
    @Test func zippyApplicativeIdentityTruncates() async {
        let v = from([1, 2, 3])
        let idFns = DeferredStream<@Sendable (Int) -> Int> { AsyncStream { c in c.yield { $0 }; c.finish() } }
        // pure(id) has one element → zip truncates v to length 1.
        #expect(await collect(applyDeferredStream(idFns, v)) == [1])
    }

    // Concat-alt monoid with the new empty identity: left/right identity + associativity.
    @Test func altLeftIdentity() async {
        let s = from([1, 2, 3])
        #expect(await collect(DeferredStream.alt(.empty(), s)) == collect(s))
    }

    @Test func altRightIdentity() async {
        let s = from([1, 2, 3])
        #expect(await collect(DeferredStream.alt(s, .empty())) == collect(s))
    }

    @Test func altAssociativity() async {
        let a = from([1]), b = from([2]), c = from([3])
        let lhs = DeferredStream.alt(DeferredStream.alt(a, b), c)
        let rhs = DeferredStream.alt(a, DeferredStream.alt(b, c))
        #expect(await collect(lhs) == collect(rhs))
    }
}

// MARK: - Publisher (lawful Functor / Monad via sequential flatMap; concat-alt monoid)

@Suite(.timeLimit(.minutes(1))) struct PublisherLawTests {
    private func from(_ xs: [Int]) -> Publisher<Int, Never> { .sequence(xs) }

    @Test func functorIdentity() async {
        let p = from([1, 2, 3])
        #expect(await collect(p.map { $0 }) == collect(p))
    }

    @Test func functorComposition() async {
        let p = from([1, 2, 3])
        #expect(await collect(p.map(inc).map(dbl)) == collect(p.map { dbl(inc($0)) }))
    }

    // Ordered monad laws use flatMap(maxPublishers: 1): plain flatMap is an unordered merge.
    @Test func monadLeftIdentity() async {
        let a = 5
        let f: @Sendable (Int) -> Publisher<Int, Never> = { .sequence([$0, $0 + 1]) }
        #expect(await collect(Publisher<Int, Never>.just(a).flatMap(maxPublishers: 1, f)) == collect(f(a)))
    }

    @Test func monadRightIdentity() async {
        let m = from([1, 2, 3])
        #expect(await collect(m.flatMap(maxPublishers: 1) { .just($0) }) == collect(m))
    }

    @Test func monadAssociativity() async {
        let m = from([1, 2])
        let f: @Sendable (Int) -> Publisher<Int, Never> = { .sequence([$0, $0 * 10]) }
        let g: @Sendable (Int) -> Publisher<Int, Never> = { .just($0 + 100) }
        let lhs = m.flatMap(maxPublishers: 1, f).flatMap(maxPublishers: 1, g)
        let rhs = m.flatMap(maxPublishers: 1) { a in f(a).flatMap(maxPublishers: 1, g) }
        #expect(await collect(lhs) == collect(rhs))
    }

    @Test func altLeftIdentity() async {
        let s = from([1, 2, 3])
        #expect(await collect(Publisher.alt(.empty(), s)) == collect(s))
    }

    @Test func altRightIdentity() async {
        let s = from([1, 2, 3])
        #expect(await collect(Publisher.alt(s, .empty())) == collect(s))
    }

    @Test func altAssociativity() async {
        let a = from([1]), b = from([2]), c = from([3])
        let lhs = Publisher.alt(Publisher.alt(a, b), c)
        let rhs = Publisher.alt(a, Publisher.alt(b, c))
        #expect(await collect(lhs) == collect(rhs))
    }
}

// MARK: - WriterT over DeferredTask (locks in the B3 log-combining fix)

@Suite(.timeLimit(.minutes(1))) struct WriterTLawTests {
    // pure for WriterT: the effect of a value with the empty (mempty) log.
    private func pureW(_ a: Int) -> DeferredTask<Writer<[String], Int>> {
        DeferredTask { Writer(a, []) }
    }

    @Test func monadLeftIdentity() async {
        let a = 5
        let f: @Sendable (Int) -> DeferredTask<Writer<[String], Int>> = { n in DeferredTask { Writer(n * 2, ["f"]) } }
        let lhs = await run(pureW(a).flatMapT(f))
        let rhs = await run(f(a))
        #expect(lhs.value == rhs.value)
        #expect(lhs.log == rhs.log) // empty <> ["f"] == ["f"] — the fix: continuation's log is kept
    }

    @Test func monadRightIdentity() async {
        let m = DeferredTask { Writer<[String], Int>(9, ["m"]) }
        let result = await run(m.flatMapT { (n: Int) in DeferredTask { Writer<[String], Int>(n, []) } })
        #expect(result.value == 9)
        #expect(result.log == ["m"]) // ["m"] <> empty == ["m"]
    }

    @Test func monadAssociativityCombinesAllLogs() async {
        let m = DeferredTask { Writer<[String], Int>(1, ["m"]) }
        let f: @Sendable (Int) -> DeferredTask<Writer<[String], Int>> = { n in DeferredTask { Writer(n + 1, ["f"]) } }
        let g: @Sendable (Int) -> DeferredTask<Writer<[String], Int>> = { n in DeferredTask { Writer(n * 3, ["g"]) } }
        let lhs = await run(m.flatMapT(f).flatMapT(g))
        let rhs = await run(m.flatMapT { a in f(a).flatMapT(g) })
        #expect(lhs.value == rhs.value)
        #expect(lhs.log == rhs.log)
        #expect(lhs.log == ["m", "f", "g"]) // all three logs accumulated in order
    }
}

// MARK: - Validation error accumulation

private enum LawErr: Error, Equatable, Sendable { case a, b }

@Suite(.timeLimit(.minutes(1))) struct ValidationLawTests {
    // Applicative accumulates BOTH errors on a double failure, in order.
    @Test func doubleFailureCombinesInOrder() async {
        let fns = DeferredTask<Validation<[LawErr], @Sendable (Int) -> Int>> { .failure([.a]) }
        let vals = DeferredTask<Validation<[LawErr], Int>> { .failure([.b]) }
        let result = await run(applyTDeferredTaskValidation(fns, vals))
        #expect(result == .failure([.a, .b]))
    }

    @Test func successThreadsValue() async {
        let fns = DeferredTask<Validation<[LawErr], @Sendable (Int) -> Int>> { .success(inc) }
        let vals = DeferredTask<Validation<[LawErr], Int>> { .success(41) }
        let result = await run(applyTDeferredTaskValidation(fns, vals))
        #expect(result == .success(42))
    }
}
