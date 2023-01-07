import Queues
import MongoKitten
import Vapor

extension Application.Queues {
   public func setupMongo(using database: MongoDatabase) async throws {
        // It's perfectly safe to call this without doing any additional checking because MongoDB will handle this for us.
        // 1. If the collection does not exist yet, mongodb will create one and add the index to it.
        // 2. If the index already exists on the collection, mongodb will just ignore the command.
        
        var index = CreateIndexes.Index(named: "job_index", keys: ["jobid": 1, "queue": 1])
        index.unique = true
        
        try await database["vapor_queue"].createIndexes([index])
    }
}

extension Application.Queues.Provider {
    public static func mongodb(_ database: MongoDatabase) -> Self {
        .init {
            $0.queues.use(custom: MongoQueuesDriver(database: database))
        }
    }
}

enum QueuesMongoError: Error {
    case missingJob
}

class MongoQueue: Queue {
    var context: QueueContext
    var mongodb: MongoDatabase
    
    init(context: QueueContext, mongodb: MongoDatabase) {
        self.context = context
        self.mongodb = mongodb
    }
    
    func get(_ id: JobIdentifier) -> EventLoopFuture<JobData> {
        let promise = context.eventLoop.makePromise(of: JobData.self)
        promise.completeWithTask {
            let job = try await self.mongodb["vapor_queue"].findOne(
                ["jobid": id.string,
                 "queue": "\(self.context.queueName.string)",
                 "status": MongoJobStatus.processing.rawValue],
                as: MongoJob.self
            )
            if let job = job {
                return job.data
            } else {
                throw QueuesMongoError.missingJob
            }
        }
        return promise.futureResult
    }
    
    func set(_ id: JobIdentifier, to data: JobData) -> EventLoopFuture<Void> {
        do {
            let job = MongoJob(
                status: MongoJobStatus.ready,
                jobid: id.string,
                queue: context.queueName.string,
                data: data,
                created: Date()
            )
            let encoded = try BSONEncoder().encode(job)
            
            let promise = context.eventLoop.makePromise(of: Void.self)
            promise.completeWithTask {
                try await self.mongodb["vapor_queue"].insert(encoded)
                return
            }
            return promise.futureResult
        } catch {
            return self.context.eventLoop.makeFailedFuture(error)
        }
    }
    
    // Mark job as completed
    func clear(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        let promise = context.eventLoop.makePromise(of: Void.self)
        promise.completeWithTask {
            let reply = try await self.mongodb["vapor_queue"].findAndModify(
                where: [
                    "jobid": id.string,
                    "queue": "\(self.context.queueName.string)",
                    "status": MongoJobStatus.processing.rawValue
                ],
                update: ["$set": ["status": MongoJobStatus.completed.rawValue]],
                returnValue: .modified
            )
            .execute()
            guard reply.ok == 1 else {
                throw reply
            }
            return
        }
        return promise.futureResult
    }
    
    // Mark oldest job as processing
    func pop() -> EventLoopFuture<JobIdentifier?> {
        let promise = context.eventLoop.makePromise(of: JobIdentifier?.self)
        
        promise.completeWithTask {
            let reply = try await self.mongodb["vapor_queue"].findAndModify(
                where: [
                    "queue": "\(self.context.queueName.string)",
                    "status": "ready"
                ],
                update: [
                    "$set": [
                        "status": MongoJobStatus.processing.rawValue
                    ]
                ],
                returnValue: .modified
            )
                .sort(["created": .ascending])
                .execute()
            
            guard reply.ok == 1, let document = reply.value else {
                return nil
            }
            let job = try BSONDecoder().decode(MongoJob.self, from: document)
            return JobIdentifier(string: job.jobid)
        }
        
        return promise.futureResult
    }
    
    // Mark jobs that can't be finished as ready and reset the created date to put it to the back of the queue.
    func push(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        let promise = context.eventLoop.makePromise(of: Void.self)
        promise.completeWithTask {
            _ = try await self.mongodb["vapor_queue"]
                .findAndModify(
                    where: [
                        "jobid": id.string,
                        "queue": "\(self.context.queueName.string)",
                        "status": "processing"
                    ],
                    update: [
                        "$set": [
                            "status": MongoJobStatus.ready.rawValue,
                            "created": Date()
                        ] as Document
                    ]
                )
                .execute()
            return
        }
        
        return promise.futureResult
    }
}

struct MongoQueuesDriver: QueuesDriver {
    internal let database: MongoDatabase
    
    init(database: MongoDatabase) {
        self.database = database
    }
    
    func makeQueue(with context: QueueContext) -> Queue {
        MongoQueue(context: context, mongodb: database)
    }
    
    func shutdown() {
    }
}

internal struct MongoJob: Codable {
    /// The current status of the job.
    var status: MongoJobStatus
    /// Unique identifier of the job.
    var jobid: String
    /// The queue this job is currently in.
    var queue: String
    /// The `JobData`.
    var data: JobData
    /// When the job was created.
    var created: Date
}

internal enum MongoJobStatus: String, Codable {
    /// The job is rerady to be picked up and executed/processed.
    case ready
    /// The job is currently being processed.
    case processing
    /// The job has completed processing.
    case completed
}
