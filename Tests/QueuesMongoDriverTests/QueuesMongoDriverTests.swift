import QueuesMongoDriver
import Queues
import XCTVapor
import MongoKitten

final class QueuesMongoDriverTests: XCTestCase {
    
    func testExample() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        let email = Email()
        app.queues.add(email)

        let mongoDatabase = try MongoDatabase.lazyConnect("mongodb://localhost:27017/queuesdriver",
                                                          on: app.eventLoopGroup.next())
        
        try app.queues.setupMongo(using: mongoDatabase)
        
        app.queues.use(.mongodb(mongoDatabase))

        app.get("send-email") { req in
            req.queue.dispatch(Email.self, .init(to: "mongo@database.driver"))
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "send-email") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        XCTAssertEqual(email.sent, [])
        try app.queues.queue.worker.run().wait()
        XCTAssertEqual(email.sent, [.init(to: "mongo@database.driver")])
    }
}

final class Email: Job {
    struct Message: Codable, Equatable {
        let to: String
    }
    
    var sent: [Message]
    
    init() {
        self.sent = []
    }
    
    func dequeue(_ context: QueueContext, _ message: Message) -> EventLoopFuture<Void> {
        self.sent.append(message)
        context.logger.info("sending email \(message)")
        return context.eventLoop.makeSucceededFuture(())
    }
}
