using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <summary>
/// One message-archive persistence destination <see cref="MessageArchiveBackgroundService"/> fans a
/// drained <see cref="MessageArchive"/> out to - <see cref="CosmosMessageArchiveSink"/> (the
/// <c>MessageArchive</c> Cosmos container) and/or <see cref="BlobMessageArchiveSink"/> (the cold-tier
/// Blob Storage archive), gated independently by <see cref="MessageArchiveOptions.CosmosDbEnabled"/>/
/// <see cref="MessageArchiveOptions.BlobEnabled"/>. Zero, one, or both may be registered - see
/// <c>MessageArchiveServiceCollectionExtensions.AddMessageArchiving</c>.
/// </summary>
public interface IMessageArchiveSink
{
    /// <summary>
    /// Persists <paramref name="entry"/> to this destination. Never throws - each implementation owns
    /// its own failure handling (retry, dead-letter fallback, or log-and-swallow), so a failure in one
    /// sink never prevents <see cref="MessageArchiveBackgroundService"/> from persisting to the others.
    /// </summary>
    Task PersistAsync(MessageArchive entry, CancellationToken cancellationToken = default);
}
