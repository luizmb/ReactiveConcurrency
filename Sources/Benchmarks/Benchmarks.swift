// SPDX-License-Identifier: Apache-2.0

import Foundation
import ReactiveConcurrency
#if canImport(Combine)
    import Combine
#endif

// Lightweight, dependency-free throughput benchmarks. Wall-clock only (no allocation profiling —
// that'd need package-benchmark + jemalloc, which complicates the cross-platform story).
// Run with: swift run -c release Benchmarks
//
// Headline question: per-operator overhead. Every operator wraps a fresh AsyncStream and spawns a
// Task per subscription, so a deep chain pays N× that. The map x1 / map x10 / baseline rows show it.

func fmt(_ value: Double, _ width: Int = 8, _ decimals: Int = 2) -> String {
    var s = String(format: "%.\(decimals)f", value)
    while s.count < width {
        s = " " + s
    }
    return s
}

func pad(_ s: String, _ width: Int) -> String {
    var r = s
    while r.count < width {
        r += " "
    }
    return r
}

func report(_ name: String, n: Int, secs: Double, extra: String) {
    let throughput = Double(n) / secs / 1_000_000
    print("\(pad(name, 34))\(pad("\(n)", 10)) elems \(fmt(secs * 1_000)) ms \(fmt(throughput, 7)) M/s   \(extra)")
}

@discardableResult
func bench(_ name: String, n: Int, _ body: @Sendable (Int) async -> Int) async -> Int {
    _ = await body(Swift.min(n, 10_000)) // warmup
    let clock = ContinuousClock()
    let start = clock.now
    let checksum = await body(n)
    report(name, n: n, secs: seconds(start.duration(to: clock.now)), extra: "chk=\(checksum)")
    return checksum
}

func seconds(_ d: Duration) -> Double {
    Double(d.components.seconds) + Double(d.components.attoseconds) / 1e18
}

@main
enum Benchmarks {
    static func main() async {
        print("ReactiveConcurrency throughput (release recommended)\n")
        await chainBenchmarks()
        await concurrentBenchmarks()
        await subjectBenchmark()
        await comparativeBenchmarks()
    }

    // Cross-library comparison: pure AsyncStream vs ReactiveConcurrency vs Combine, on identity
    // (source -> sink) and a 3x map chain. Combine is synchronous (no executor hop per element),
    // so it sets the upper bound; the async models pay an AsyncStream suspension per element.
    static func comparativeBenchmarks() async {
        let n = 1_000_000
        print("\nCross-library comparison (\(n) elements):\n")
        await comparativeIdentity(n)
        await comparativeMap3(n)
    }

    static func comparativeIdentity(_ n: Int) async {
        await bench("AsyncStream identity", n: n) { n in
            var sum = 0
            let (s, c) = AsyncStream<Int>.makeStream()
            let consumer = Task {
                var a = 0
                for await v in s {
                    a &+= v
                }
                return a
            }
            for i in 0..<n {
                c.yield(i)
            }
            c.finish()
            sum = await consumer.value
            return sum
        }
        await bench("ReactiveConcurrency identity", n: n) { n in
            let acc = Sum(); let box = CancellableBox()
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                let p: ReactiveConcurrency.Publisher<Int, Never> = (0..<n).publisher
                box.c = p.sink(
                    receiveCompletion: { _ in box.c = nil; cont.resume() },
                    receiveValue: { acc.add($0) }
                )
            }
            return acc.v
        }
        #if canImport(Combine)
            await bench("Combine identity", n: n) { n in
                let acc = Sum()
                await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                    let box = CombineBox()
                    box.c = (0..<n).publisher.sink(
                        receiveCompletion: { _ in box.c = nil; cont.resume() },
                        receiveValue: { acc.add($0) }
                    )
                }
                return acc.v
            }
        #endif
    }

    static func comparativeMap3(_ n: Int) async {
        await bench("AsyncStream map x3", n: n) { n in
            var sum = 0
            let (s, c) = AsyncStream<Int>.makeStream()
            let mapped = s.map { $0 &+ 1 }.map { $0 &+ 1 }.map { $0 &+ 1 }
            let consumer = Task {
                var a = 0
                for await v in mapped {
                    a &+= v
                }
                return a
            }
            for i in 0..<n {
                c.yield(i)
            }
            c.finish()
            sum = await consumer.value
            return sum
        }
        await bench("ReactiveConcurrency map x3", n: n) { n in
            let acc = Sum(); let box = CancellableBox()
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                let p: ReactiveConcurrency.Publisher<Int, Never> = (0..<n).publisher
                let chained = p.map { $0 &+ 1 }.map { $0 &+ 1 }.map { $0 &+ 1 }
                box.c = chained.sink(
                    receiveCompletion: { _ in box.c = nil; cont.resume() },
                    receiveValue: { acc.add($0) }
                )
            }
            return acc.v
        }
        #if canImport(Combine)
            await bench("Combine map x3", n: n) { n in
                let acc = Sum()
                await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                    let box = CombineBox()
                    // Pin to Combine's concrete Sequence type to disambiguate `.publisher` from RC.
                    let src: Publishers.Sequence<Range<Int>, Never> = (0..<n).publisher
                    let chained = src.eraseToAnyPublisher().map { $0 &+ 1 }.map { $0 &+ 1 }.map { $0 &+ 1 }
                    box.c = chained.sink(
                        receiveCompletion: { _ in box.c = nil; cont.resume() },
                        receiveValue: { acc.add($0) }
                    )
                }
                return acc.v
            }
        #endif
    }

    static func chainBenchmarks() async {
        await bench("baseline AsyncStream", n: 1_000_000) { n in
            var sum = 0
            let s = AsyncStream<Int> { c in
                for i in 0..<n {
                    c.yield(i)
                }
                c.finish()
            }
            for await v in s {
                sum &+= v
            }
            return sum
        }
        await bench("Publisher.sequence.values", n: 1_000_000) { n in
            var sum = 0
            for await v in Publisher<Int, Never>.sequence(0..<n).values {
                sum &+= v
            }
            return sum
        }
        await bench("map x1", n: 1_000_000) { n in
            var sum = 0
            for await v in Publisher<Int, Never>.sequence(0..<n).map({ $0 &+ 1 }).values {
                sum &+= v
            }
            return sum
        }
        await bench("map x10", n: 1_000_000) { n in
            var sum = 0
            var p = Publisher<Int, Never>.sequence(0..<n)
            for _ in 0..<10 {
                p = p.map { $0 &+ 1 }
            }
            for await v in p.values {
                sum &+= v
            }
            return sum
        }
        await bench("filter (keep even)", n: 1_000_000) { n in
            var sum = 0
            for await v in Publisher<Int, Never>.sequence(0..<n).filter({ $0.isMultiple(of: 2) }).values {
                sum &+= v
            }
            return sum
        }
        await bench("scan (running sum)", n: 1_000_000) { n in
            var last = 0
            for await v in Publisher<Int, Never>.sequence(0..<n).scan(0, { $0 &+ $1 }).values {
                last = v
            }
            return last
        }
    }

    static func concurrentBenchmarks() async {
        await bench("flatMap (1 inner each)", n: 100_000) { n in
            var sum = 0
            let p = Publisher<Int, Never>.sequence(0..<n).flatMap { Publisher<Int, Never>.just($0 &+ 1) }
            for await v in p.values {
                sum &+= v
            }
            return sum
        }
        await bench("merge x4", n: 1_000_000) { n in
            var sum = 0
            let q = n / 4
            let merged = Publisher<Int, Never>.merge([.sequence(0..<q), .sequence(0..<q), .sequence(0..<q), .sequence(0..<q)])
            for await v in merged.values {
                sum &+= v
            }
            return sum
        }
        await bench("zip2", n: 1_000_000) { n in
            var sum = 0
            let zipped = Publisher<Int, Never>.sequence(0..<n).zip(Publisher<Int, Never>.sequence(0..<n))
            for await pair in zipped.values {
                sum &+= pair.0 &+ pair.1
            }
            return sum
        }
    }

    // PassthroughSubject end-to-end: one consumer drains while values are sent.
    static func subjectBenchmark() async {
        let n = 500_000
        let counter = Counter()
        let subject = ReactiveConcurrency.PassthroughSubject<Int, Never>()
        let consumer = Task {
            for await _ in subject.eraseToPublisher().values {
                counter.bump()
            }
        }
        for _ in 0..<12 {
            await Task.yield()
        } // let the consumer subscribe

        let clock = ContinuousClock()
        let start = clock.now
        for i in 0..<n {
            subject.send(i)
        }
        subject.send(completion: .finished)
        while counter.value < n {
            await Task.yield()
        }
        report("PassthroughSubject send+drain", n: n, secs: seconds(start.duration(to: clock.now)), extra: "recv=\(counter.value)")
        consumer.cancel()
    }
}

final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var _v = 0
    var value: Int { lock.withLock { _v } }
    func bump() { lock.withLock { _v += 1 } }
}

// Lock-free accumulator for the single-consumer sink callbacks (matches the AsyncStream loop's
// plain `&+=`, so no per-element lock skews the cross-library comparison).
final class Sum: @unchecked Sendable { var v = 0; func add(_ x: Int) { v &+= x } }

// Retain boxes: AnyCancellable cancels on deinit, so the subscription must be held until completion.
final class CancellableBox: @unchecked Sendable { var c: ReactiveConcurrency.AnyCancellable? }
#if canImport(Combine)
    final class CombineBox: @unchecked Sendable { var c: Combine.AnyCancellable? }
#endif
