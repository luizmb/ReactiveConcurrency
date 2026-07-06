// swift-tools-version: 6.3
import PackageDescription

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
    dependencies: [
        .package(url: "https://github.com/luizmb/FP.git", from: "1.14.0"),
        .package(url: "https://github.com/luizmb/Hourglass.git", from: "0.7.0"),
        .package(url: "https://github.com/swiftlang/swift-docc-plugin", from: "1.4.0"),
    ],
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
