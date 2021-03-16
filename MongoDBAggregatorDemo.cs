using System;
using System.Collections.Generic;
using System.Text;

namespace Demos
{
    public static class SomeAggregator
    {
        private static FilterDefinition<SomeEntity> getPublicFilter()
        {
            return Builders<SomeEntity>.Filter.And(
                Builders<SomeEntity>.Filter.Eq(x => x.scope, SomeScope.Global),
                Builders<SomeEntity>.Filter.Lte(x => x.someTimeFirld, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()));
        }
        private static FilterDefinition<SomeEntity> getPrivateFilter(string[] requiredParents)
        {
            return Builders<SomeEntity>.Filter.And(
                Builders<SomeEntity>.Filter.In(x => x.parent, requiredParents),
                Builders<SomeEntity>.Filter.Eq(x => x.scope, SomeScope.Building),
                Builders<SomeEntity>.Filter.Lte(x => x.someTimeFirld, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()));
        }
        private static SortDefinition<SomeEntity> getDefaultSort()
        {
            return Builders<SomeEntity>.Sort.Descending(x => x.someTimeFirld);
        }
        private static BsonDocument addIsLikedByField(string requesterId)
        {
            var input = GE.PropertyName<SomeEntity>(x => x.likedBy, false);
            var isLikedField = GE.PropertyName<SomeEntityProjection>(x => x.isLiked, false);
            return new BsonDocument("$addFields", MongoHelper.addFieldAnyElementInTemplate(input,
                string.Empty, requesterId, isLikedField));
        }
        public static Task<List<SomeEntityProjection>> buildPublicNewsList(
            this IMongoCollection<SomeEntity> collection,
            string requesterId, int page = 0, int pageSize = 10)
        {
            var filter = getPublicFilter(); var sort = getDefaultSort();
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred)
                .Aggregate().Match(filter).Sort(sort)
                .Skip(pageSize * page).Limit(pageSize)
                .AppendStage<SomeEntityProjection>(addIsLikedByField(requesterId))
                .Project<SomeEntityProjection>(MongoHelper.IQProjectionBuilder<SomeEntityProjection>());
            return query.ToListAsync();
        }

        public static Task<List<SomeEntityProjection>> buildPrivateEntityList(
            this IMongoCollection<SomeEntity> collection,
            string requesterId, string[] requiredBuildings, int page = 0, int pageSize = 10)
        {
            var filter = getPrivateFilter(requiredBuildings); var sort = getDefaultSort();
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred).Aggregate()
                .Match(filter).Sort(sort)
                .Skip(pageSize * page).Limit(pageSize)
                .AppendStage<SomeEntityProjection>(addIsLikedByField(requesterId))
                .Project<SomeEntityProjection>(MongoHelper.IQProjectionBuilder<SomeEntityProjection>());
            return query.ToListAsync();
        }

        public static Task<SomeEntityProjection> extractCertainEntity(
            this IMongoCollection<SomeEntity> collection, string newsId,
            string requesterId)
        {
            var filter = Builders<SomeEntity>.Filter.Eq(x => x._id, newsId);
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred).Aggregate().Match(filter)
                .AppendStage<SomeEntityProjection>(addIsLikedByField(requesterId))
                .Project<SomeEntityProjection>(MongoHelper.IQProjectionBuilder<SomeEntityProjection>());
            return query.FirstOrDefaultAsync();
        }
    }
}
