// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "LongLiveCombine",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v11)
    ],
    products: [
        .library(name: "LongLiveCombine", targets: ["LongLiveCombine"])
    ],
    dependencies: [
        .package(path: "../FP")
    ],
    targets: [
        .target(
            name: "LongLiveCombine",
            dependencies: [
                .product(name: "CoreFP", package: "FP")
            ]
        ),
        .testTarget(
            name: "LongLiveCombineTests",
            dependencies: ["LongLiveCombine"]
        )
    ]
)
