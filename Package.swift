// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "LongLiveCombine",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v11),
        .visionOS(.v2),
    ],
    products: [
        .library(name: "ReactiveConcurrency", targets: ["ReactiveConcurrency"]),
        .library(name: "ReactiveConcurrencyOperators", targets: ["ReactiveConcurrencyOperators"]),
        .library(name: "ReactiveConcurrencyTransformers", targets: ["ReactiveConcurrencyTransformers"]),
    ],
    dependencies: [
        .package(path: "../FP")
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
                .product(name: "CoreFP", package: "FP"),
                .product(name: "CoreFPOperators", package: "FP"),
            ]
        ),
        .target(
            name: "ReactiveConcurrencyTransformers",
            dependencies: [
                "ReactiveConcurrency",
                .product(name: "DataStructure", package: "FP"),
            ]
        ),
        .testTarget(
            name: "LongLiveCombineTests",
            dependencies: ["ReactiveConcurrency"]
        )
    ]
)
