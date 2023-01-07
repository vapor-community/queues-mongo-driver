// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "queues-mongo-driver",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "QueuesMongoDriver",
            targets: ["QueuesMongoDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/vapor.git", from: "4.0.0"),
        .package(url: "https://github.com/vapor/queues.git", from: "1.0.0"),
        .package(url: "https://github.com/OpenKitten/MongoKitten.git", from: "7.2.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "QueuesMongoDriver",
            dependencies: [
                .product(name: "Vapor", package: "vapor"),
                .product(name: "Queues", package: "queues"),
                .product(name: "MongoKitten", package: "MongoKitten"),
            ]),
        .testTarget(
            name: "QueuesMongoDriverTests",
            dependencies: [
                .target(name: "QueuesMongoDriver"),
                .product(name: "XCTVapor", package: "vapor")
            ])
    ]
)
