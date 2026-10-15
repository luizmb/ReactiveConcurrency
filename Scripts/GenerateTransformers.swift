#!/usr/bin/env swift
// SPDX-License-Identifier: Apache-2.0
//
// Generates the newtype monad-transformer stacks over ReactiveConcurrency's effect types
// (`DeferredTaskTResult`, `ReaderTPublisher`, …), following FP 3's model: one struct per stack,
// wrapping the whole nested value, with its own `map` / `<£>` / `<*>` / `>>-`.
//
// Run from the package root:
//
//     swift Scripts/GenerateTransformers.swift
//
// The output is checked in. Every file under `Sources/ReactiveConcurrencyTransformers/Generated/`
// and `Sources/ReactiveConcurrencyOperators/Generated/` is owned by this script: the directories are
// wiped and rewritten on each run, so never edit them by hand.
//
// Adding a stack: add one `Stack(...)` line to `inventory` below and re-run. The struct members
// delegate to the stack's internal nested-type functions (`mapTDeferredTaskResult`,
// `applyReaderPublisher`, …); `Impl` picks their names, which follow one of two families.

import Foundation

// MARK: - Model

/// One layer of a stack. The same layer behaves differently as the outer and as the inner layer.
enum Layer: String {
    case array = "Array"
    case optional = "Optional"
    case result = "Result"
    case either = "Either"
    case writer = "Writer"
    case validation = "Validation"
    case reader = "Reader"
    case stateful = "Stateful"
    case deferredTask = "DeferredTask"
    case deferredStream = "DeferredStream"
    case publisher = "Publisher"

    var name: String { rawValue }

    /// Lifting property on the outer type (`task.deferredTaskT`).
    var liftName: String { name.prefix(1).lowercased() + name.dropFirst() + "T" }

    /// Context parameters this layer adds as the OUTER layer.
    var outerParams: [Param] {
        switch self {
        case .deferredTask,
             .deferredStream: []
        case .publisher: [Param("Failure", "Error")]
        case .reader: [Param("Env")]
        case .stateful: [Param("S")]
        default: [] // never an outer layer
        }
    }

    /// Context parameters this layer adds as the INNER layer.
    var innerParams: [Param] {
        switch self {
        case .array,
             .optional,
             .deferredTask,
             .deferredStream: []
        case .result,
             .publisher: [Param("E", "Error")]
        case .either: [Param("L", "Sendable")]
        case .writer: [Param("W", "Monoid")]
        case .validation: [Param("E", "Semigroup")]
        default: [] // never an inner layer
        }
    }

    /// The layer's type around `inner`, using the given context parameter names.
    func type(around inner: String, _ ctx: [String]) -> String {
        switch self {
        case .array: "[\(inner)]"
        case .optional: "\(inner)?"
        case .result: "Result<\(inner), \(ctx[0])>"
        case .either: "Either<\(ctx[0]), \(inner)>"
        case .writer: "Writer<\(ctx[0]), \(inner)>"
        case .validation: "Validation<\(ctx[0]), \(inner)>"
        case .reader: "Reader<\(ctx[0]), \(inner)>"
        case .stateful: "Stateful<\(ctx[0]), \(inner)>"
        case .deferredTask: "DeferredTask<\(inner)>"
        case .deferredStream: "DeferredStream<\(inner)>"
        case .publisher: "Publisher<\(inner), \(ctx[0])>"
        }
    }

    /// How the outer type is extended with the lifting property.
    var lift: Lift {
        switch self {
        case .deferredTask: Lift(ext: "DeferredTask", elem: "Success", ctx: [], convert: { "map { Success.\($0)($0) }" })
        case .deferredStream: Lift(ext: "DeferredStream", elem: "Element", ctx: [], convert: { "map { Element.\($0)($0) }" })
        case .publisher: Lift(ext: "Publisher", elem: "Output", ctx: ["Failure"], convert: { "map { Output.\($0)($0) }" })
        case .reader: Lift(ext: "Reader", elem: "Output", ctx: ["Environment"], convert: { "mapReader { Output.\($0)($0) }" })
        case .stateful: Lift(ext: "Stateful", elem: "A", ctx: ["S"], convert: { "mapStateful { A.\($0)($0) }" })
        // Never an outer layer.
        default: Lift(ext: name, elem: "A", ctx: [], convert: { $0 })
        }
    }

    /// The inner-shape protocol constraining the lifting extension, and its identity conversion.
    var shape: Shape {
        switch self {
        case .array: Shape(proto: "ArrayLike", convert: "asArray", ctx: [], value: "Element")
        case .optional: Shape(proto: "OptionalLike", convert: "asOptional", ctx: [], value: "Wrapped")
        case .result: Shape(proto: "ResultLike", convert: "asResult", ctx: ["Failure"], value: "Success")
        case .either: Shape(proto: "EitherLike", convert: "asEither", ctx: ["Left"], value: "Right")
        case .writer: Shape(proto: "WriterLike", convert: "asWriter", ctx: ["Log"], value: "Value")
        case .validation: Shape(proto: "ValidationLike", convert: "asValidation", ctx: ["Errors"], value: "Value")
        case .deferredTask: Shape(proto: "DeferredTaskLike", convert: "asDeferredTask", ctx: [], value: "Success")
        case .deferredStream: Shape(proto: "DeferredStreamLike", convert: "asDeferredStream", ctx: [], value: "Element")
        case .publisher: Shape(proto: "PublisherLike", convert: "asPublisher", ctx: ["Failure"], value: "Output")
        // Never an inner layer.
        default: Shape(proto: name, convert: "", ctx: [], value: "A")
        }
    }
}

struct Param {
    let name: String
    let constraint: String?

    init(_ name: String, _ constraint: String? = nil) {
        self.name = name
        self.constraint = constraint
    }

    /// `Error`, `Monoid`, `Semigroup` and `Sendable` all imply `Sendable`.
    var impliesSendable: Bool { constraint != nil }

    var declaration: String { constraint.map { "\(name): \($0)" } ?? name }

    var sendableDeclaration: String { "\(name): \(constraint ?? "Sendable")" }
}

struct Lift {
    let ext: String
    let elem: String
    let ctx: [String]
    let convert: (String) -> String
}

struct Shape {
    let proto: String
    let convert: String
    let ctx: [String]
    let value: String
}

enum Kind {
    /// Functor + applicative, no lawful monad: `TransformerStack`.
    case applicative
    /// Functor + applicative + monad: `MonadT`.
    case monad
}

struct Stack {
    let outer: Layer
    let inner: Layer
    let kind: Kind

    init(_ outer: Layer, _ inner: Layer, _ kind: Kind) {
        self.outer = outer
        self.inner = inner
        self.kind = kind
    }

    var name: String { "\(outer.name)T\(inner.name)" }
    var qualifiedName: String { "ReactiveConcurrencyTransformers.\(name)" }
    var infix: String { "\(outer.name)\(inner.name)" }
    var layersDoc: String { "outer `\(outer.name)`, inner `\(inner.name)`" }

    /// Inner context parameters, renamed when they collide with an outer one.
    var innerParams: [Param] {
        let outerNames = Set(outer.outerParams.map(\.name))
        return inner.innerParams.map { param in
            guard outerNames.contains(param.name) else { return param }
            return Param(["E": "Err"][param.name] ?? "Inner\(param.name)", param.constraint)
        }
    }

    /// Every ReactiveConcurrency effect type requires a `Sendable` value.
    var params: [Param] { outer.outerParams + innerParams + [Param("A", "Sendable")] }

    var contextNames: [String] { params.dropLast().map(\.name) }

    func innerType(_ value: String, _ ctx: [String]) -> String {
        inner.type(around: value, Array(ctx.suffix(innerParams.count)))
    }

    func outerType(_ value: String, _ ctx: [String]) -> String {
        outer.type(around: innerType(value, ctx), Array(ctx.prefix(outer.outerParams.count)))
    }

    func iType(_ value: String = "A") -> String { innerType(value, contextNames) }

    /// The struct type applied to the given value type, e.g. `ReaderTPublisher<Env, E, B>`.
    func applied(_ value: String, qualified: Bool = false) -> String {
        "\(qualified ? qualifiedName : name)<\((contextNames + [value]).joined(separator: ", "))>"
    }

    /// Haskell escape-hatch name, same rule as FP: by the outer layer for Reader / Stateful / streams,
    /// else by the inner layer for Optional / Either / Result / Writer, else by the outer layer.
    var escapeHatch: String {
        switch outer {
        case .reader: return "mapReaderT"
        case .stateful: return "mapStateT"
        case .publisher: return "mapPublisherT"
        case .deferredStream: return "mapDeferredStreamT"
        default: break
        }
        switch inner {
        case .optional: return "mapMaybeT"
        case .either,
             .result: return "mapExceptT"
        case .writer: return "mapWriterT"
        default: return "map\(outer.name)T"
        }
    }

    /// The where-clause shared by every algebraic member: all parameters Sendable.
    var memberWhere: String {
        let missing = params.filter { !$0.impliesSendable }.map { "\($0.name): Sendable" }
        return missing.isEmpty ? "" : " where \(missing.joined(separator: ", "))"
    }

    /// The internal nested-type functions the members delegate to. Stacks over a plain container
    /// (Array, Either, Optional, Result, Validation) use the `…T<Outer><Inner>` free functions; the
    /// Writer / Reader / Stateful stacks use `mapT` / `flatMapT` methods and unsuffixed free functions.
    var impl: Impl {
        if [.array, .either, .optional, .result, .validation].contains(inner) {
            return Impl(
                map: "mapT\(infix)(fn, rawValue)",
                apply: "applyT\(infix)(ff.rawValue, fa.rawValue)",
                liftA2: "liftA2T\(infix)(fn)(lhs.rawValue, rhs.rawValue)",
                seqRight: nil,
                seqLeft: nil,
                flatMap: "flatMapT\(infix)(rawValue) { fn($0).rawValue }"
            )
        }
        return Impl(
            map: "rawValue.mapT(fn)",
            apply: "apply\(infix)(ff.rawValue, fa.rawValue)",
            liftA2: "liftA2\(infix)(fn)(lhs.rawValue, rhs.rawValue)",
            seqRight: "seqRight\(infix)(rawValue, rhs.rawValue)",
            seqLeft: "seqLeft\(infix)(rawValue, rhs.rawValue)",
            flatMap: "rawValue.flatMapT { fn($0).rawValue }"
        )
    }
}

struct Impl {
    let map: String
    let apply: String
    let liftA2: String
    /// `nil`: derived from `liftA2`.
    let seqRight: String?
    let seqLeft: String?
    let flatMap: String
}

// MARK: - Inventory

/// Every transformer stack of the library. Kinds follow FP 3 (Haskell as the source of truth): a
/// list inside a non-commutative effect (`ListT` done wrong) and a state threaded outside the effect
/// have no lawful monad, so those stacks are applicative-only.
let inventory: [Stack] = [
    // DeferredTask
    Stack(.deferredTask, .array, .applicative),
    Stack(.deferredTask, .either, .monad),
    Stack(.deferredTask, .optional, .monad),
    Stack(.deferredTask, .result, .monad),
    Stack(.deferredTask, .validation, .applicative),
    Stack(.deferredTask, .writer, .monad),
    // DeferredStream
    Stack(.deferredStream, .array, .applicative),
    Stack(.deferredStream, .either, .monad),
    Stack(.deferredStream, .optional, .monad),
    Stack(.deferredStream, .result, .monad),
    Stack(.deferredStream, .validation, .applicative),
    Stack(.deferredStream, .writer, .monad),
    // Publisher
    Stack(.publisher, .array, .applicative),
    Stack(.publisher, .either, .monad),
    Stack(.publisher, .optional, .monad),
    Stack(.publisher, .result, .monad),
    Stack(.publisher, .validation, .applicative),
    Stack(.publisher, .writer, .monad),
    // Reader
    Stack(.reader, .deferredTask, .monad),
    Stack(.reader, .deferredStream, .monad),
    Stack(.reader, .publisher, .monad),
    // Stateful
    Stack(.stateful, .deferredTask, .applicative),
    Stack(.stateful, .deferredStream, .applicative),
    Stack(.stateful, .publisher, .applicative)
]

// MARK: - Emission helpers

let header = """
// SPDX-License-Identifier: Apache-2.0
// Generated by Scripts/GenerateTransformers.swift; do not edit.
// Regenerate with `swift Scripts/GenerateTransformers.swift` from the package root.
"""

func file(imports: [String], body: String) -> String {
    header + "\n\n" + imports.map { "import \($0)" }.joined(separator: "\n") + "\n\n" + body + "\n"
}

/// Renames whole-word identifiers (`A` → `B`, `Env` → `Env2`) in a type expression.
func rename(_ text: String, _ map: [String: String]) -> String {
    var output = ""
    var word = ""
    func flush() {
        output += map[word] ?? word
        word = ""
    }
    for char in text {
        if char.isLetter || char.isNumber || char == "_" {
            word.append(char)
        } else {
            flush()
            output.append(char)
        }
    }
    flush()
    return output
}

// MARK: - Struct file

func structFile(_ stack: Stack) -> String {
    let name = stack.name
    let generics = stack.params.map(\.declaration).joined(separator: ", ")
    let conformance = stack.kind == .monad ? "MonadT" : "TransformerStack"
    let renames = Dictionary(uniqueKeysWithValues: stack.params.map { ($0.name, $0.name == "A" ? "B" : $0.name + "2") })
    let hatchGenerics = stack.params.map { Param(renames[$0.name] ?? $0.name, $0.constraint).declaration }
        .joined(separator: ", ")
    let hatchStruct = rename("\(name)<\(stack.params.map(\.name).joined(separator: ", "))>", renames)
    let kindDoc = switch stack.kind {
    case .monad: "A lawful monad (`MonadT`): functor, applicative and monad surface."
    case .applicative: "No lawful monad exists for this stack, so it is applicative-only (`TransformerStack`), like Haskell's `Compose`."
    }
    let outerCtx = Array(stack.contextNames.prefix(stack.outer.outerParams.count))

    let parts = [
        """
        /// The `\(stack.outerType("A", stack.contextNames))` stack (\(stack.layersDoc)) as its own type.
        ///
        /// \(kindDoc)
        /// Wrap a nested value with `init(_:)` or the `.\(stack.outer.liftName)` lifting property, and leave with `rawValue`.
        public struct \(name)<\(generics)>: \(conformance) {
            /// The inner layer of the stack.
            public typealias I = \(stack.iType())
            /// The whole nested value.
            public typealias O = \(stack.outer.type(around: "I", outerCtx))

            /// The wrapped nested value.
            public let rawValue: O

            /// Wraps a nested value.
            /// - Parameter rawValue: The nested value.
            public init(rawValue: O) {
                self.rawValue = rawValue
            }

            /// Wraps a nested value (unlabelled, for point-free use).
            /// - Parameter rawValue: The nested value.
            public init(_ rawValue: O) {
                self.rawValue = rawValue
            }

            /// Escape hatch: transforms the whole nested value, reaching the full API of the underlying types.
            /// `\(stack.escapeHatch) (fmap f)` covers "transform the inner value".
            /// - Parameter fn: Transformation of the nested value.
            /// - Returns: The stack over the transformed nested value.
            public func \(stack.escapeHatch)<\(hatchGenerics)>(
                _ fn: (O) -> \(hatchStruct).O
            ) -> \(hatchStruct) {
                \(hatchStruct)(fn(rawValue))
            }
        }
        """,
        sendableExtension(stack),
        functorExtension(stack),
        applicativeExtension(stack),
        stack.kind == .monad ? monadExtension(stack) : nil,
        liftExtension(stack)
    ].compactMap(\.self)

    return file(imports: ["CoreFP", "DataStructure", "ReactiveConcurrency"], body: parts.joined(separator: "\n\n"))
}

func sendableExtension(_ stack: Stack) -> String {
    // Reader / Stateful store a `@Sendable` closure; the effect types are Sendable for a Sendable value.
    if stack.outer == .reader || stack.outer == .stateful { return "extension \(stack.name): Sendable {}" }
    let conditions = stack.params.filter { !$0.impliesSendable }.map { "\($0.name): Sendable" }
    return conditions.isEmpty
        ? "extension \(stack.name): Sendable {}"
        : "extension \(stack.name): Sendable where \(conditions.joined(separator: ", ")) {}"
}

func functorExtension(_ stack: Stack) -> String {
    let selfA = stack.applied("A")
    let target = stack.applied("B")
    return """
    // MARK: - Functor

    public extension \(stack.name)\(stack.memberWhere) {
        /// Maps the value inside both layers.
        /// fmap :: (a -> b) -> t a -> t b
        /// - Parameter fn: Transformation of the innermost value.
        /// - Returns: The stack with the transformed value.
        func map<B: Sendable>(
            _ fn: @escaping @Sendable (A) -> B
        ) -> \(target) {
            \(target)(\(stack.impl.map))
        }

        /// Curried, point-free form of ``map(_:)``.
        /// fmap :: (a -> b) -> t a -> t b
        /// - Parameter fn: Transformation of the innermost value.
        /// - Returns: A function mapping `fn` over a stack.
        static func fmap<B: Sendable>(
            _ fn: @escaping @Sendable (A) -> B
        ) -> @Sendable (\(selfA)) -> \(target) {
            { $0.map(fn) }
        }
    }
    """
}

func applicativeExtension(_ stack: Stack) -> String {
    let selfA = stack.applied("A")
    let selfB = stack.applied("B")
    let impl = stack.impl
    // A monad stack derives its applicative from its bind (Haskell's `ap`), so `<*>` always agrees
    // with `>>-`: the left effect runs first and, on a short-circuit, the right one never runs.
    let derived = stack.kind == .monad
    let applyBody = derived ? "ff.flatMap { function in fa.map { function($0) } }" : "\(selfA)(\(impl.apply))"
    let liftA2Body = derived ? "lhs.flatMap { left in rhs.map { right in fn(left, right) } }" : "\(selfA)(\(impl.liftA2))"
    let seqRightBody = derived
        ? "flatMap { _ in rhs }"
        : impl.seqRight.map { "\(selfB)(\($0))" } ?? "\(selfB).liftA2 { (_: A, right: B) in right }(self, rhs)"
    let seqLeftBody = derived
        ? "flatMap { left in rhs.map { _ in left } }"
        : impl.seqLeft.map { "\(selfA)(\($0))" } ?? "\(selfA).liftA2 { (left: A, _: B) in left }(self, rhs)"
    return """
    // MARK: - Applicative

    public extension \(stack.name)\(stack.memberWhere) {
        /// Lifts a value into both layers.
        /// pure :: a -> t a
        /// - Parameter value: The value to lift.
        /// - Returns: The minimal stack holding `value`.
        static func pure(_ value: A) -> \(selfA) {
            \(selfA)(.pure(I.pure(value)))
        }

        /// Applies the functions inside a stack to the values inside another.
        /// (<*>) :: t (a -> b) -> t a -> t b
        /// - Parameters:
        ///   - ff: The stack of functions.
        ///   - fa: The stack of arguments.
        /// - Returns: The stack of results.
        static func apply<Input: Sendable>(
            _ ff: \(stack.applied("@Sendable (Input) -> A")),
            _ fa: \(stack.applied("Input"))
        ) -> \(selfA) {
            \(applyBody)
        }

        /// Lifts a binary function to work on two stacks.
        /// liftA2 :: (a1 -> a2 -> a) -> t a1 -> t a2 -> t a
        /// - Parameter fn: The binary function.
        /// - Returns: A function combining two stacks with `fn`.
        static func liftA2<A1: Sendable, A2: Sendable>(
            _ fn: @escaping @Sendable (A1, A2) -> A
        ) -> @Sendable (\(stack.applied("A1")), \(stack.applied("A2"))) -> \(selfA) {
            { lhs, rhs in
                \(liftA2Body)
            }
        }

        /// Sequences two stacks, keeping the right result.
        /// (*>) :: t a -> t b -> t b
        /// - Parameter rhs: The stack whose result is kept.
        /// - Returns: The combined stack.
        func seqRight<B: Sendable>(
            _ rhs: \(selfB)
        ) -> \(selfB) {
            \(seqRightBody)
        }

        /// Sequences two stacks, keeping the left result.
        /// (<*) :: t a -> t b -> t a
        /// - Parameter rhs: The stack whose result is discarded.
        /// - Returns: The combined stack.
        func seqLeft<B: Sendable>(
            _ rhs: \(selfB)
        ) -> \(selfA) {
            \(seqLeftBody)
        }
    }
    """
}

func monadExtension(_ stack: Stack) -> String {
    let selfA = stack.applied("A")
    let selfB = stack.applied("B")
    return """
    // MARK: - Monad

    public extension \(stack.name)\(stack.memberWhere) {
        /// Monadic bind over the whole stack.
        /// (>>=) :: t a -> (a -> t b) -> t b
        /// - Parameter fn: Continuation returning the next stack.
        /// - Returns: The bound stack.
        func flatMap<B: Sendable>(
            _ fn: @escaping @Sendable (A) -> \(selfB)
        ) -> \(selfB) {
            \(selfB)(\(stack.impl.flatMap))
        }

        /// Curried, point-free form of ``flatMap(_:)``.
        /// (=<<) :: (a -> t b) -> t a -> t b
        /// - Parameter fn: Continuation returning the next stack.
        /// - Returns: A function binding a stack to `fn`.
        static func bind<B: Sendable>(
            _ fn: @escaping @Sendable (A) -> \(selfB)
        ) -> @Sendable (\(selfA)) -> \(selfB) {
            { $0.flatMap(fn) }
        }

        /// Kleisli composition (left-to-right).
        /// (>=>) :: (a0 -> t a) -> (a -> t b) -> a0 -> t b
        /// - Parameters:
        ///   - fn1: The first step.
        ///   - fn2: The second step.
        /// - Returns: The composed step.
        static func kleisli<A0, B: Sendable>(
            _ fn1: @escaping @Sendable (A0) -> \(selfA),
            _ fn2: @escaping @Sendable (A) -> \(selfB)
        ) -> @Sendable (A0) -> \(selfB) {
            { fn1($0).flatMap(fn2) }
        }

        /// Kleisli composition (right-to-left).
        /// (<=<) :: (a -> t b) -> (a0 -> t a) -> a0 -> t b
        /// - Parameters:
        ///   - fn2: The second step.
        ///   - fn1: The first step.
        /// - Returns: The composed step.
        static func kleisliBack<A0, B: Sendable>(
            _ fn2: @escaping @Sendable (A) -> \(selfB),
            _ fn1: @escaping @Sendable (A0) -> \(selfA)
        ) -> @Sendable (A0) -> \(selfB) {
            { fn1($0).flatMap(fn2) }
        }
    }
    """
}

func liftExtension(_ stack: Stack) -> String {
    let lift = stack.outer.lift
    let shape = stack.inner.shape
    let elem = lift.elem
    let args = lift.ctx + shape.ctx.map { "\(elem).\($0)" } + ["\(elem).\(shape.value)"]
    let target = "\(stack.name)<\(args.joined(separator: ", "))>"
    var conditions = ["\(elem): \(shape.proto)"]
    for (param, arg) in zip(stack.params, args) where param.constraint != nil && !lift.ctx.contains(arg) {
        conditions.append("\(arg): \(param.constraint ?? "Sendable")")
    }
    if stack.outer == .reader || stack.outer == .stateful {
        conditions.append("\(lift.ctx[0]): Sendable")
    }
    return """
    // MARK: - Lifting

    public extension \(lift.ext) where \(conditions.joined(separator: ", ")) {
        /// Lifts this nested value into the ``\(stack.name)`` stack.
        var \(stack.outer.liftName): \(target) {
            \(target)(\(lift.convert(shape.convert)))
        }
    }
    """
}

// MARK: - Operator file

func operatorFile(_ stack: Stack) -> String {
    let ctx = stack.params.dropLast().map(\.sendableDeclaration)
    func generics(_ names: [String]) -> String { (ctx + names.map { "\($0): Sendable" }).joined(separator: ", ") }
    let kleisliGenerics = (ctx + ["A0", "A: Sendable", "B: Sendable"]).joined(separator: ", ")
    // Qualified: on Apple, FP declares Combine stacks with the same names (`PublisherTResult`, …).
    let fa = stack.applied("A", qualified: true)
    let fb = stack.applied("B", qualified: true)
    let tag = stack.name
    var ops = [
        """
        /// (<$>) :: (a -> b) -> \(tag) a -> \(tag) b
        public func <£> <\(generics(["A", "B"]))>(
            _ fn: @escaping @Sendable (A) -> B,
            _ fa: \(fa)
        ) -> \(fb) {
            fa.map(fn)
        }

        /// (<&>) :: \(tag) a -> (a -> b) -> \(tag) b
        public func <&> <\(generics(["A", "B"]))>(
            _ fa: \(fa),
            _ fn: @escaping @Sendable (A) -> B
        ) -> \(fb) {
            fn <£> fa
        }

        /// ($>) :: \(tag) a -> b -> \(tag) b
        public func £> <\(generics(["A", "B"]))>(
            _ fa: \(fa),
            _ value: B
        ) -> \(fb) {
            fa.map(const(value))
        }

        /// (<$) :: b -> \(tag) a -> \(tag) b
        public func <£ <\(generics(["A", "B"]))>(
            _ value: B,
            _ fa: \(fa)
        ) -> \(fb) {
            fa £> value
        }

        /// (<*>) :: \(tag) (a -> b) -> \(tag) a -> \(tag) b
        public func <*> <\(generics(["A", "B"]))>(
            _ ff: \(stack.applied("@Sendable (A) -> B", qualified: true)),
            _ fa: \(fa)
        ) -> \(fb) {
            \(fb).apply(ff, fa)
        }

        /// (*>) :: \(tag) a -> \(tag) b -> \(tag) b
        public func *> <\(generics(["A", "B"]))>(
            _ lhs: \(fa),
            _ rhs: \(fb)
        ) -> \(fb) {
            lhs.seqRight(rhs)
        }

        /// (<*) :: \(tag) a -> \(tag) b -> \(tag) a
        public func <* <\(generics(["A", "B"]))>(
            _ lhs: \(fa),
            _ rhs: \(fb)
        ) -> \(fa) {
            lhs.seqLeft(rhs)
        }
        """
    ]
    if stack.kind == .monad {
        ops.append("""
        /// (>>=) :: \(tag) a -> (a -> \(tag) b) -> \(tag) b
        public func >>- <\(generics(["A", "B"]))>(
            _ fa: \(fa),
            _ fn: @escaping @Sendable (A) -> \(fb)
        ) -> \(fb) {
            fa.flatMap(fn)
        }

        /// (=<<) :: (a -> \(tag) b) -> \(tag) a -> \(tag) b
        public func -<< <\(generics(["A", "B"]))>(
            _ fn: @escaping @Sendable (A) -> \(fb),
            _ fa: \(fa)
        ) -> \(fb) {
            fa >>- fn
        }

        /// (>=>) :: (a0 -> \(tag) a) -> (a -> \(tag) b) -> a0 -> \(tag) b
        public func >=> <\(kleisliGenerics)>(
            _ fn1: @escaping @Sendable (A0) -> \(fa),
            _ fn2: @escaping @Sendable (A) -> \(fb)
        ) -> @Sendable (A0) -> \(fb) {
            \(fa).kleisli(fn1, fn2)
        }

        /// (<=<) :: (a -> \(tag) b) -> (a0 -> \(tag) a) -> a0 -> \(tag) b
        public func <=< <\(kleisliGenerics)>(
            _ fn2: @escaping @Sendable (A) -> \(fb),
            _ fn1: @escaping @Sendable (A0) -> \(fa)
        ) -> @Sendable (A0) -> \(fb) {
            fn1 >=> fn2
        }
        """)
    }
    let imports = ["CoreFP", "CoreFPOperators", "DataStructure", "ReactiveConcurrency", "ReactiveConcurrencyTransformers"]
    return file(imports: imports, body: ops.joined(separator: "\n\n"))
}

// MARK: - Main

let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
let fileManager = FileManager.default

func generatedDirectory(_ module: String) -> URL {
    root.appendingPathComponent("Sources/\(module)/Generated", isDirectory: true)
}

let names = inventory.map(\.name)
if Set(names).count != names.count {
    FileHandle.standardError.write(Data("Duplicate stack in inventory\n".utf8))
    exit(1)
}

for module in ["ReactiveConcurrencyTransformers", "ReactiveConcurrencyOperators"] {
    let directory = generatedDirectory(module)
    try? fileManager.removeItem(at: directory)
    try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
}

for stack in inventory {
    let structURL = generatedDirectory("ReactiveConcurrencyTransformers").appendingPathComponent("\(stack.name).swift")
    try structFile(stack).write(to: structURL, atomically: true, encoding: .utf8)
    let operatorURL = generatedDirectory("ReactiveConcurrencyOperators").appendingPathComponent("\(stack.name)+Operators.swift")
    try operatorFile(stack).write(to: operatorURL, atomically: true, encoding: .utf8)
}

print("Generated \(inventory.count) transformer stacks.")
