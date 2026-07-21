using System.Text;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <inheritdoc cref="IMessageArchiveSink"/>
/// <remarks>
/// Archives every entry as JSON to the cold-tier <see cref="BlobStorageOptions.MessageArchiveContainerName"/>
/// container, named <c>{Timestamp:yyyyMMddHHmmssffffff}_{CorrelationId}_{Category}_{Guid}.json</c> - unlike
/// <c>Persistence.CosmosDb.Audit.ColdBlobAuditSink</c>, only one destination container, since a
/// <see cref="MessageArchive"/> has a single logical archive target rather than the audit trail's
/// separate archive/request-audit containers. The write failure is logged and swallowed, never
/// rethrown - the message archive is a diagnostic aid, not the durability boundary
/// (integration-resiliency.instructions.md §5), so a cold-storage outage must not disrupt the rest of
/// <see cref="MessageArchiveBackgroundService"/>'s drain loop or any other registered
/// <see cref="IMessageArchiveSink"/>. Registered only when <see cref="MessageArchiveOptions.BlobEnabled"/>
/// is <see langword="true"/> (see <c>MessageArchiveServiceCollectionExtensions.AddMessageArchiving</c>).
/// </remarks>
public sealed class BlobMessageArchiveSink(
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.ColdTierKey)] IFileStore coldFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ILogger<BlobMessageArchiveSink> logger) : IMessageArchiveSink
{
    /// <inheritdoc />
    public async Task PersistAsync(MessageArchive entry, CancellationToken cancellationToken = default)
    {
        var fileName = $"{entry.Timestamp:yyyyMMddHHmmssffffff}_{entry.CorrelationId}_{entry.Category}_{Guid.NewGuid()}.json";

        try
        {
            var json = JsonConvert.SerializeObject(entry);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await coldFileStore.UploadAsync(blobStorageOptions.Value.MessageArchiveContainerName, fileName, stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to archive message archive entry {Id}/{Category} to cold-tier Blob Storage container {DestinationContainer} - continuing without it.",
                entry.Id, entry.Category, blobStorageOptions.Value.MessageArchiveContainerName);
        }
    }
}
