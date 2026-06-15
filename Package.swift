// swift-tools-version: 6.2
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
        .package(url: "https://github.com/luizmb/FP.git", from: "1.10.0"),
        .package(url: "https://github.com/luizmb/Hourglass.git", from: "0.2.1"),
        .package(url: "https://github.com/swiftlang/swift-docc-plugin", from: "1.4.0"),
    ],
    targets: [
        .target(
            name: "ReactiveConcurrency",
            dependencies: []
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
                .product(name: "CoreFPOperators", package: "FP"),
                .product(name: "Hourglass", package: "Hourglass"),
            ]
        )
    ]
)
