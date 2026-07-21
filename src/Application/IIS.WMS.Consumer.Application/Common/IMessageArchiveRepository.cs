using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Port for message-archive persistence (cosmos-db.instructions.md §5), implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.MessageArchiveRepository</c>. Only <see cref="UpsertAsync"/>
/// is exposed - like <c>IOrderArchiveRepository</c>, this data is an unordered write-once diagnostic
/// record with no read-modify-write step, so there is no ETag-guarded replace/patch surface to expose here.
/// </summary>
public interface IMessageArchiveRepository
{
    /// <summary>
    /// Unconditionally overwrites the item at <paramref name="entity"/>'s partition key with its
    /// current state - last write wins, no ETag check. Correct here because a redelivered message
    /// archiving twice under the same deterministic id is expected, not concurrently contested state.
    /// </summary>
    /// <param name="entity">Record to persist.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<MessageArchive> UpsertAsync(MessageArchive entity, CancellationToken cancellationToken = default);
}
