using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MongoDBDemos
{
    public interface IMongoRepositoryCollection
    {
        public IMongoCollection<T> getCollection<T>();
    }
    internal interface IMongoDefaultRequests : IMongoRepositoryCollection
    {
        Task<IClientSessionHandle> initTransactionAsync(ClientSessionOptions options = null);

        Task upsertBatchAsync<T>(IEnumerable<T> upsertObjects, IClientSessionHandle? session) where T : IMongoEntity;

        Task<List<T>> getRandomObjectsAsync<T>(int size);
        Task<Result> getSingleObjectByIdWithProjectionAsync<Result>(string id);
        Task<List<Result>> getMultipleObjectsByIdWithProjectionAsync<Result>(string[] ids) where Result : IMongoEntity;
        Task mergeDocumentAsync<OriginalObjectType>(string documentId, OriginalObjectType patchDocument, IClientSessionHandle? session);
        Task mergeDocumentsBatchAsync<OriginalObjectType>(OriginalObjectType[] patchDocument, IClientSessionHandle? session) where OriginalObjectType : IMongoEntity;
        Task mergeDocumentsBatchAsync<OriginalObjectType>(string[] ids, OriginalObjectType[] patchDocument, IClientSessionHandle? session);
    }
    public abstract class BaseMongoRepo : IMongoDefaultRequests
    {
        protected readonly MongoService mService;

        protected BaseMongoRepo(MongoService mService)
        {
            this.mService = mService;
        }

        abstract public IMongoCollection<T> getCollection<T>();

        public Task<IClientSessionHandle> initTransactionAsync(ClientSessionOptions options = null)
        {
            return mService.client.StartSessionAsync(options);
        }

        public Task<List<T>> getRandomObjectsAsync<T>(int size)
        {
            return getCollection<T>().AsQueryable().Sample(size).ToListAsync();
        }

        public Task<List<Result>> getMultipleObjectsByIdWithProjectionAsync<Result>(string[] ids) where Result : IMongoEntity
        {
            var filter = Builders<Result>.Filter.In(x => x._id, ids);
            return buildProjection(filter).ToListAsync();
        }

        public Task mergeDocumentAsync<OriginalObjectType>(string documentId, OriginalObjectType patchDocument, IClientSessionHandle? session)
        {
            var filter = Builders<OriginalObjectType>.Filter.Eq(GE.PropertyName<MongoEntity>(x => x._id, false), new ObjectId(documentId));
            var update = new BsonDocument() { { "$set", patchDocument.ToBsonDocument() } };
            return session == null ?
                getCollection<OriginalObjectType>().UpdateOneAsync(filter, update)
                :
                getCollection<OriginalObjectType>().UpdateOneAsync(session, filter, update);
        }

        public Task mergeDocumentsBatchAsync<OriginalObjectType>(OriginalObjectType[] patchDocument, IClientSessionHandle? session) where OriginalObjectType : IMongoEntity
        {
            string[] ids = patchDocument.Where(x => x._id != null).Distinct().Select(x => x._id).ToArray();
            return mergeDocumentsBatchAsync(ids, patchDocument, session);
        }

        public Task mergeDocumentsBatchAsync<OriginalObjectType>(string[] ids, OriginalObjectType[] patchDocument, IClientSessionHandle? session)
        {
            if (ids.Length != patchDocument.Length)
                throw new InvalidOperationException($"{nameof(ids)} collection and {nameof(patchDocument)} have different size");
            var bulkOps = new List<WriteModel<OriginalObjectType>>();
            var idName = GE.PropertyName<MongoEntity>(x => x._id, false);
            for (int i = 0; i < ids.Length; i++)
            {
                var update = new BsonDocument() { { "$set", patchDocument[i].ToBsonDocument() } };
                bulkOps.Add(new UpdateOneModel<OriginalObjectType>(
                    Builders<OriginalObjectType>.Filter.Eq(idName, new ObjectId(ids[i])), update) { IsUpsert = false });
            }
            return session == null
                ? getCollection<OriginalObjectType>().BulkWriteAsync(bulkOps)
                : getCollection<OriginalObjectType>().BulkWriteAsync(session, bulkOps);
        }

        private IAggregateFluent<Result> buildProjection<Result>(FilterDefinition<Result> filter)
        {
            var projection = MongoHelper.IQProjectionBuilder<Result>();
            var projectionStage = new BsonDocument("$project", projection);
            return getCollection<Result>().Aggregate().Match(filter).AppendStage<Result>(projectionStage);
        }
    }

    internal interface ISubGroupedInterface
    {
        public Task ExecuteSomeAction1(string entityId, IClientSessionHandle transaction);
        public Task ExecuteSomeAction2(string entityId, string? rejectionComment, IClientSessionHandle transaction);
        public Task ExecuteSomeAction3(string entityId, string assigneeId, IClientSessionHandle transaction);
    }
    internal interface IInternalRepositoryGroupedInterface : ISubGroupedInterface
    {
        public Task<List<SomeAggregationResponse>> PaginateRequestsForAssetsAsync(string[] assetsIds, int page = 0, int pageSize = 10);
        public Task<SomeAggregationResponse> GetCertainEntityAsync(string entityId);
    }
    public class SomewhatRepositoryTwo : BaseMongoRepo, IInternalRepositoryGroupedInterface
    {
        public const string collectionName = "some-collection-name";

        public SomewhatRepositoryTwo(MongoService mService) : base(mService)
        {
            ensureIndexes();
        }

        private void ensureIndexes()
        {
            var createRequestList = new List<CreateIndexModel<SomeModelOne>>
            {
                new CreateIndexModel<SomeModelOne>(
                new IndexKeysDefinitionBuilder<SomeModelOne>()
                    .Descending(x => x.timeA).Descending(x => x.timeB).Descending(x => x.timeC),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<SomeModelOne>(
                    new IndexKeysDefinitionBuilder<SomeModelOne>().Descending(x => x.authorId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<SomeModelOne>(
                    new IndexKeysDefinitionBuilder<SomeModelOne>().Descending(x => x.otherId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<SomeModelOne>(
                    new IndexKeysDefinitionBuilder<SomeModelOne>().Descending(x => x.anotherId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<SomeModelOne>(
                    new IndexKeysDefinitionBuilder<SomeModelOne>().Descending(x => x.performedId),
                new CreateIndexOptions() { Unique = false })
            };

            getCollection<SomeModelOne>().Indexes.CreateManyAsync(createRequestList);
        }

        public override IMongoCollection<T> getCollection<T>()
        {
            return mService.getMainDatabase.GetCollection<T>(collectionName);
        }

        public Task<List<SomeAggregationResponse>> PaginateRequestsForAssetsAsync(string[] assetsIds, int page = 0, int pageSize = 10)
        {
            return SomeAggregatorClass.buildentitiesForAssets(assetsIds, this.getCollection<SomeAggregationResponse>(), page, pageSize);
        }

        public Task<SomeAggregationResponse> GetCertainEntityAsync(string entityId)
        {
            return SomeAggregatorClass.getCertainentity(entityId, getCollection<SomeAggregationResponse>());
        }

        public Task ExecuteSomeAction1(string entityId, IClientSessionHandle transaction)
        {
            var filter = Builders<SomeModelOne>.Filter.And(
                Builders<SomeModelOne>.Filter.Eq(x => x._id, entityId),
                Builders<SomeModelOne>.Filter.Eq(x => x.status, SomeModelOne.entitiestatus.Assigned));

            var update = Builders<SomeModelOne>.Update.Set(x => x.status, SomeModelOne.entitiestatus.Closed)
                .Set(x => x.closedTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds());

            return (transaction != null) ?
                getCollection<SomeModelOne>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<SomeModelOne>().UpdateOneAsync(filter, update);
        }

        public Task ExecuteSomeAction2(string entityId, string? rejectionComment, IClientSessionHandle transaction)
        {
            var filter = Builders<SomeModelOne>.Filter.And(
                Builders<SomeModelOne>.Filter.Eq(x => x._id, entityId),
                Builders<SomeModelOne>.Filter.Lte(x => x.status, SomeModelOne.entitiestatus.Assigned));

            var update = Builders<SomeModelOne>.Update.Set(x => x.status, SomeModelOne.entitiestatus.Rejected)
                .Set(x => x.rejectionTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
                .Set(x => x.rejectionComment, rejectionComment);

            return (transaction != null) ?
                getCollection<SomeModelOne>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<SomeModelOne>().UpdateOneAsync(filter, update);
        }

        public Task ExecuteSomeAction3(string entityId, string assigneeId, IClientSessionHandle transaction)
        {
            var filter = Builders<SomeModelOne>.Filter.And(
                Builders<SomeModelOne>.Filter.Eq(x => x._id, entityId),
                Builders<SomeModelOne>.Filter.Ne(x => x.status, SomeModelOne.entitiestatus.Rejected),
                Builders<SomeModelOne>.Filter.Lte(x => x.status, SomeModelOne.entitiestatus.Closed));

            var update = Builders<SomeModelOne>.Update.Set(x => x.status, SomeModelOne.entitiestatus.Assigned)
                .Set(x => x.acceptedTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
                .Set(x => x.executerId, assigneeId);

            return (transaction != null) ?
                getCollection<SomeModelOne>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<SomeModelOne>().UpdateOneAsync(filter, update);
        }
    }
}
