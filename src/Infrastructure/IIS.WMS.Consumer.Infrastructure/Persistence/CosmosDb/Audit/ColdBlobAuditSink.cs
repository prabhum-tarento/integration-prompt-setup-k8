using System.Text;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <inheritdoc cref="IAuditSink"/>
/// <remarks>
/// Archives every entry as JSON to the cold-tier <see cref="BlobStorageOptions.AuditArchiveContainerName"/>
/// container, named <c>{yyyy}/{MM}/{dd}/{entry.Id}.json</c> (same date-partitioned convention as the
/// general cold-tier request/response audit, integration-resiliency.instructions.md §5, keyed by entry id
/// rather than correlation id since several entries can share one correlation id). A write failure is
/// logged and swallowed, never rethrown - the audit trail is a diagnostic aid, not the durability
/// boundary (integration-resiliency.instructions.md §5), so a cold-storage outage must not disrupt the
/// rest of <see cref="AuditBackgroundService"/>'s drain loop or any other registered <see cref="IAuditSink"/>.
/// Registered only when <see cref="AuditOptions.ColdStorageEnabled"/> is <see langword="true"/> (see
/// <c>AuditServiceCollectionExtensions.AddAuditTrail</c>).
/// </remarks>
public sealed class ColdBlobAuditSink(
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.ColdTierKey)] IFileStore coldFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ILogger<ColdBlobAuditSink> logger) : IAuditSink
{
    /// <inheritdoc />
    public async Task PersistAsync(AuditEntry entry, CancellationToken cancellationToken = default)
    {
        try
        {
            var json = JsonConvert.SerializeObject(entry);
            var blobName = $"{entry.TimestampUtc:yyyy}/{entry.TimestampUtc:MM}/{entry.TimestampUtc:dd}/{entry.Id}.json";

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await coldFileStore.UploadAsync(
                blobStorageOptions.Value.AuditArchiveContainerName, blobName, stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to archive audit entry {Id} for {ContainerName}/{EntityId} (operation {Operation}) to cold-tier Blob Storage - continuing without it.",
                entry.Id, entry.ContainerName, entry.EntityId, entry.Operation);
        }
    }
}
