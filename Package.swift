// swift-tools-version: 6.3
import PackageDescription

var dependencies: [Package.Dependency] = [
    .package(url: "https://github.com/luizmb/FP.git", from: "2.0.0"),
    .package(url: "https://github.com/luizmb/Hourglass.git", from: "0.7.0"),
]

// swift-docc-plugin only generates documentation (run on macOS in CI via the Documentation
// workflow). Its command plugin is built by `swift build` on Windows and fails there, so exclude
// the dependency on Windows hosts — it is not needed to build or test the package.
#if !os(Windows)
    dependencies.append(.package(url: "https://github.com/swiftlang/swift-docc-plugin", from: "1.4.0"))
#endif

let package = Package(
    name: "ReactiveConcurrency",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .tvOS(.v16),
        .watchOS(.v9),
        .visionOS(.v1),
    ],
    products: [
        .library(name: "ReactiveConcurrency", targets: ["ReactiveConcurrency"]),
        .library(name: "ReactiveConcurrencyOperators", targets: ["ReactiveConcurrencyOperators"]),
        .library(name: "ReactiveConcurrencyTransformers", targets: ["ReactiveConcurrencyTransformers"]),
    ],
    dependencies: dependencies,
    targets: [
        .target(
            name: "ReactiveConcurrency",
            dependencies: [
                .product(name: "Hourglass", package: "Hourglass"),
            ]
        ),
        .target(
            name: "ReactiveConcurrencyOperators",
            dependencies: [
                "ReactiveConcurrency",
                "ReactiveConcurrencyTransformers",
                .product(name: "CoreFP", package: "FP"),
                .product(name: "CoreFPOperators", package: "FP"),
                .product(name: "DataStructure", package: "FP"),
            ]
        ),
        .target(
            name: "ReactiveConcurrencyTransformers",
            dependencies: [
                "ReactiveConcurrency",
                .product(name: "CoreFP", package: "FP"),
                .product(name: "DataStructure", package: "FP"),
            ]
        ),
        .testTarget(
            name: "ReactiveConcurrencyTests",
            dependencies: [
                "ReactiveConcurrency",
                "ReactiveConcurrencyOperators",
                "ReactiveConcurrencyTransformers",
                .product(name: "CoreFP", package: "FP"),
                .product(name: "CoreFPOperators", package: "FP"),
                .product(name: "DataStructure", package: "FP"),
                .product(name: "DataStructureOperators", package: "FP"),
                .product(name: "Hourglass", package: "Hourglass"),
            ]
        ),
        .executableTarget(
            name: "Benchmarks",
            dependencies: [
                "ReactiveConcurrency",
                .product(name: "Hourglass", package: "Hourglass"),
            ]
        ),
    ]
)
