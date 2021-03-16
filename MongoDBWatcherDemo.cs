using System;
using System.Threading.Tasks;

namespace Demos
{
    public abstract class BaseCollectionWatcher : IDisposable
    {
        protected CancellationTokenSource revoker;
        protected bool disposed = false;
        protected readonly Logger logger;

        private readonly SomeAnotherRepository anotherRepository;
        protected readonly UserRepository userRepository;

        protected ISomeAbstractionLevelProxy someAbstractionLevelProxy;
        protected IWebSocketsInterface webSocketsInterface;

        protected BaseCollectionWatcher(Logger logger,
            ISomeAbstractionLevelProxy someAbstractionLevelProxy, IWebSocketsInterface webSocketsInterface,
            UserRepository userRepository, SomeAnotherRepository anotherRepository)
        {
            this.userRepository = userRepository; this.anotherRepository = anotherRepository;
            this.logger = logger;
            this.updateSenderProxy = updateSenderProxy; this.webSocketsInterface = webSocketsInterface;
        }
        ~BaseCollectionWatcher()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                revoker.Cancel();
                revoker.Dispose();
            }
            disposed = true;
        }

        /// <summary>
        /// Get users with enabled notifications within building id and required access
        /// </summary>
        /// <param name="buildingId"></param>
        /// <param name="accessAnyBitFilter">One of this access</param>
        /// <returns></returns>
        protected async Task<List<MongoEntity>> getUsersWithEnabledNotificationsInSomeScope(
            string someEntityBuildingId, long accessAnyBitFilter)
        {
            var usersInBuilding = await anotherRepository.getRelatedUsersInBuilding(buildingId, accessAnyBitFilter);
            var usersInBuildingWithNotifications = await userRepository.filterUsersWithEnabledNotifications(
                usersInBuilding.Select(x => x._id), (long)Notifications.entities);
            return usersInBuildingWithNotifications;
        }

        protected Task cofigureWatcher<WatchType>(
            IMongoCollection<WatchType> collectionToMonitor,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            ChangeStreamOperationType operationType,
            ChangeStreamFullDocumentOption changeStreamDocumentOption = ChangeStreamFullDocumentOption.UpdateLookup,
            byte secondsToWait = 2)
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<WatchType>>().Match(x => x.OperationType == operationType);
            return buildWatcher(pipeline, changeStreamDocumentOption, secondsToWait, watchFunction, collectionToMonitor);
        }

        private async Task buildWatcher<WatchType>(
            PipelineDefinition<ChangeStreamDocument<WatchType>, ChangeStreamDocument<WatchType>> pipeline,
            ChangeStreamFullDocumentOption changeStreamDocumentOption,
            byte secondsToWait,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            IMongoCollection<WatchType> collectionToMonitor)
        {
            var options = new ChangeStreamOptions()
            {
                FullDocument = changeStreamDocumentOption,
                MaxAwaitTime = TimeSpan.FromSeconds(secondsToWait)
            };
            BsonDocument resumer = null;
            while (!revoker.IsCancellationRequested)
            {
                try
                {
                    if (resumer != null) options.ResumeAfter = resumer;
                    using var cursor = await collectionToMonitor.WatchAsync(pipeline, options, revoker.Token);
                    await cursor.ForEachAsync(watchFunction, revoker.Token);
                    resumer = cursor.GetResumeToken();
                }
                catch (Exception ex)
                {
                    logger.Fatal(ex);
                }
            }
        }

        protected Task cofigureWatcher<WatchType>(
            IMongoCollection<WatchType> collectionToMonitor,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            string watchFilter,
            ChangeStreamFullDocumentOption changeStreamDocumentOption = ChangeStreamFullDocumentOption.UpdateLookup,
            byte secondsToWait = 2)
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<WatchType>>().Match(watchFilter);
            return buildWatcher(pipeline, changeStreamDocumentOption, secondsToWait, watchFunction, collectionToMonitor);
        }

        /// <summary>
        /// Generic method for revoking updates by _id
        /// </summary>
        /// <param name="deletedEntity"></param>
        /// <returns></returns>
        protected Task processNotificableEntityWasDeleted<EntityType>(ChangeStreamDocument<EntityType> deletedEntity)
        {
            var deletedEntityId = deletedEntity.DocumentKey["_id"].ToString();
            if (deletedEntityId == null)
            {
                logger.Fatal($"document without id was received", deletedEntity);
                return Task.CompletedTask;
            }
            return updateSenderProxy.revokeNotificationForObject(deletedEntityId);
        }
    }
    /// <summary>
    /// Continiously watching entity collection and sending notification to required entities
    /// </summary>
    public sealed class SomeEntityCollectionWatcher : BaseCollectionWatcher
    {
        private readonly SomewhatRepositoryOne somewhatRepositoryOne;
        private readonly SomewhatRepositoryTwo SomewhatRepositoryTwo;
        private readonly SomewhatRepositoryThree somewhatRepositoryThree;

        //private readonly Task insertWatcher;
        //private readonly Task updateWatcher;
        //private readonly Task deleteWatcher;

        private readonly Task globalWatcher;

        public SomeEntityCollectionWatcher(
            SomewhatRepositoryOne somewhatRepositoryOne, SomeAnotherRepository anotherRepository,
            SomewhatRepositoryTwo SomewhatRepositoryTwo, UserRepository userRepository,
            SomewhatRepositoryThree somewhatRepositoryThree,
            ISomeAbstractionLevelProxy someAbstractionLevelProxy, IWebSocketsInterface websocketsInterface,
            Logger logger)
            : base(logger, updateSenderProxy, websocketsInterface, userRepository, anotherRepository)
        {
            this.somewhatRepositoryOne = somewhatRepositoryOne; this.SomewhatRepositoryTwo = SomewhatRepositoryTwo;
        }

        private IMongoCollection<SomeModelOne> getCollection()
        {
            return SomewhatRepositoryTwo.getCollection<SomeModelOne>().WithReadPreference(ReadPreference.Secondary).WithReadConcern(ReadConcern.Majority);
        }

        private Task GlobalWatch()
        {
            string filter = "{ operationType: { $in: [ 'replace', 'insert', 'update', 'delete' ] } }";
            return cofigureWatcher(getCollection(), brancher, filter, ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Launch required action depending on performed action (delete/insert/update)
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private Task Brancher(ChangeStreamDocument<SomeModelOne> entity)
        {
            switch (entity.OperationType)
            {
                case ChangeStreamOperationType.Delete:
                    return processNotificableEntityWasDeleted(entity);
                case ChangeStreamOperationType.Insert:
                    return ProcessentityInsert(entity);
                case ChangeStreamOperationType.Update:
                case ChangeStreamOperationType.Replace:
                    return ProcessentityUpdated(entity);
                default:
                    logger.Fatal($"{nameof(entity.OperationType)} is not implemented");
                    return Task.CompletedTask;
            }
        }

        #region model creation

        /// <summary>
        /// Stage A.0: Init watcher on `insert` operation
        /// </summary>
        /// <returns></returns>
        private Task WatchInsertCollection()
        {
            return cofigureWatcher(getCollection(), processentityInsert, ChangeStreamOperationType.Insert,
                ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Stage A.1: Process entity one
        /// </summary>
        /// <param name="newentity"></param>
        /// <returns></returns>
        private Task ProcessentityInsert(ChangeStreamDocument<SomeModelOne> newentity)
        {
            bool isAdminCompetency = (newentity.FullDocument.positionId == null);
            if (isAdminCompetency)
                return sendNotificationsToAdmins(newentity.FullDocument);
            else
                return sendNotificationsToPositionsOwners(newentity.FullDocument);
        }

        /// <summary>
        /// Stage A.1.1: Send not targeted entity to building administrators
        /// </summary>
        /// <param name="createdentity"></param>
        /// <returns></returns>
        private async Task SendNotificationsToAdmins(SomeModelOne createdentity)
        {
            var buildingId = (await somewhatRepositoryOne.getBuildingIdByApartId(createdentity.apartId)).buildingId;
            var adminMask = AccessMaskHelper.BuildMask(sAdmin: true, admin: true);
            var adminsWithNotification = await getUsersWithEnabledNotificationsInSomeScope(buildingId, adminMask);
            adminsWithNotification.AsParallel().ForAll(user => sendentityCreatedNotification(createdentity, buildingId, user));
        }

        /// <summary>
        /// Stage A.1.2: Send targeted entity to staff which assigned to certain position
        /// </summary>
        /// <param name="createdentity"></param>
        /// <returns></returns>
        private async Task SendNotificationsToPositionsOwners(SomeModelOne createdentity)
        {
            if (createdentity.positionId == null)
            {
                logger.Fatal($"{nameof(SomeEntityCollectionWatcher)} wtf, impossible entity passed into func `sendNotificationsToPositionsOwners`");
                return;
            } // Thread should continue
            var users = await positionsRepository.getPersonalForCertainPosition(createdentity.positionId);
            var buildingId = (await somewhatRepositoryOne.getBuildingIdByApartId(createdentity.apartId)).buildingId;
            if (users.relatedPersonal.Length > 0)
            {
                var usersWithEnabledNotifications = await userRepository.filterUsersWithEnabledNotifications(
                    users.relatedPersonal, (long)Notifications.entities);
                usersWithEnabledNotifications.AsParallel().WithDegreeOfParallelism(4)
                    .ForAll(user => sendentityCreatedNotification(createdentity, buildingId, user));
            }
        }

        private void SendentityCreatedNotification(SomeModelOne createdentity, string someEntityBuildingId, IMongoEntity user)
        {
            var update = new UpdateModel()
            {
                updateType = UpdateType.entityCreated,
                parentId = createdentity._id,
                message = $"New entity created: {createdentity.header}",
                buildingId = buildingId,
                updateOwnerId = user._id
            };
            updateSenderProxy.sendNotificationsToCertainUserSync(update, null);
        }

        #endregion

        #region entity updating

        /// <summary>
        /// Stage B.0: Init update watcher
        /// </summary>
        /// <returns></returns>
        private Task WatchentityUpdated()
        {
            string filter = "{ operationType: { $in: [ 'replace', 'update' ] } }";
            return cofigureWatcher(getCollection(), processentityUpdated, filter, ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Stage B.1: Branch algo for choosing required notification type
        /// </summary>
        /// <param name="updatedentity"></param>
        /// <returns></returns>
        private async Task ProcessentityUpdated(ChangeStreamDocument<SomeModelOne> updatedentity)
        {
            if (updatedentity.OperationType == ChangeStreamOperationType.Update
                || updatedentity.OperationType == ChangeStreamOperationType.Replace)
            {
                var buildingId = (await somewhatRepositoryOne.getBuildingIdByApartId(updatedentity.FullDocument.apartId)).buildingId;
                switch (updatedentity.FullDocument.status)
                {
                    case SomeModelOne.entitiestatus.Rejected:
                        await sendentityRejectedStatus(updatedentity.FullDocument, buildingId);
                        return;
                    case SomeModelOne.entitiestatus.Finished:
                        await sendentityFinishedStatus(updatedentity.FullDocument, buildingId);
                        return;
                    case SomeModelOne.entitiestatus.Assigned:
                        await sendentityInProgress(updatedentity.FullDocument, buildingId, true);
                        await sendentityInProgress(updatedentity.FullDocument, buildingId);
                        return;
                    case SomeModelOne.entitiestatus.Reviewed:
                    case SomeModelOne.entitiestatus.Closed:
                        await sendentityInProgress(updatedentity.FullDocument, buildingId);
                        return;
                    default:
                        logger.Warn($"entity status {nameof(updatedentity.FullDocument.status)} is not supported in `processentityUpdated`");
                        return;
                }
            }
        }

        /// <summary>
        /// Stage B.1.1: Sending entity is finished update
        /// </summary>
        /// <param name="updatedentity"></param>
        /// <param name="buildingId"></param>
        /// <returns></returns>
        private async Task SendentityFinishedStatus(SomeModelOne updatedentity, string someEntityBuildingId)
        {
            if (updatedentity.executerId == null)
            {
                logger.Fatal($"WTF, how entity w/o {nameof(updatedentity.executerId)} passed into `sendentityFinishedStatus`");
                return; // base thread should not be ended
            }
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedentity.executerId,
                updateType = UpdateType.entitiestatusChanged,
                parentId = updatedentity._id,
                message = $"entity {updatedentity.header} closed with mark {updatedentity.feedback.mark}"
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
        }

        /// <summary>
        /// Stage B.1.2: Sending entity in progress update (reviewed, assigned, closed)
        /// Author`s related notifications
        /// </summary>
        /// <param name="updatedentity"></param>
        /// <param name="buildingId"></param>
        /// <param name="isAssigned"></param>
        /// <returns></returns>
        private async Task SendentityInProgress(SomeModelOne updatedentity, string someEntityBuildingId, bool isAssigned = false)
        {
            string newStatus = "unknown";
            switch (updatedentity.status)
            {
                case SomeModelOne.entitiestatus.Reviewed:
                    newStatus = "reviewed";
                    break;
                case SomeModelOne.entitiestatus.Assigned:
                    newStatus = "assigned";
                    break;
                case SomeModelOne.entitiestatus.Closed:
                    newStatus = "closed";
                    break;
                default:
                    logger.Warn($"Status {nameof(updatedentity.status)} is not supported `sendentityInProgress`");
                    break;
            }
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedentity.executerId,
                updateType = UpdateType.entitiestatusChanged,
                parentId = updatedentity._id,
                message = $"entity {updatedentity.header} changed status to {newStatus}" // TODO: Translate
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
            if (isAssigned)
            {
                if (updatedentity.executerId == null)
                {
                    logger.Fatal($"`sendentityInProgress` isAssigned was true, but there is no executer",
                        JsonConvert.SerializeObject(updatedentity, Formatting.Indented));
                    return;
                }
                update.updateType = UpdateType.entityWasAssignedToYou;
                update.updateOwnerId = updatedentity.executerId; update.message = $"You was assigned to {updatedentity.header}";
                await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
            }
        }

        /// <summary>
        /// Stage B.1.3: Sending notifications / updates for rejected entity
        /// </summary>
        /// <param name="updatedentity"></param>
        /// <param name="buildingId"></param>
        /// <returns></returns>
        private async Task SendentityRejectedStatus(SomeModelOne updatedentity, string someEntityBuildingId)
        {
            var additionalMessage = (updatedentity.rejectionComment != null) ?
                $" with message {updatedentity.rejectionComment}" : string.Empty;
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedentity.authorId,
                updateType = UpdateType.entitiestatusChanged,
                parentId = updatedentity._id,
                message = $"entity {updatedentity.header} was rejected{additionalMessage}"
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
        }

        #endregion

        #region entity cancelled / deleted

        /// <summary>
        /// Stage C.0: Revoke updates / notification, if entity was deleted / cancelled
        /// </summary>
        /// <returns></returns>
        private Task WatchentityDeleted()
        {
            return cofigureWatcher(getCollection(), processNotificableEntityWasDeleted<SomeModelOne>,
                ChangeStreamOperationType.Delete, ChangeStreamFullDocumentOption.Default, 5);
        }

        #endregion

        protected override void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                //insertWatcher.Dispose();
                //deleteWatcher.Dispose();
                //updateWatcher.Dispose();
                globalWatcher.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
