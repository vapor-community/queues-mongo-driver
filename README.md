# QueuesMongoDriver

## A MongoDB driver for Vapor Queues

## Getting Started

To install queues-mongo-driver add the following dependency to your `Package.swift`:

`.package(url: "https://github.com/vapor-community/queues-mongo-driver.git", from: "1.0.0"),`


This driver depends on [MongoKitten](https://github.com/OpenKitten/MongoKitten) so to configure the driver we need an instance of a `MongoDatabase`. Ideally during app startup or in your `configure.swift`:

```swift
import QueuesMongoDriver
import MongoKitten

func configure(app: Application) throws {
  let mongoDatabase = try MongoDatabase.lazyConnect("mongodb://localhost:27017/my-database", on: app.eventLoopGroup.next())
  
  // Setup Indexes for the Job Schema for performance (Optional)
  try app.queues.setupMongo(using: mongoDatabase)
  app.queues.use(.mongodb(mongoDatabase))
}
```
