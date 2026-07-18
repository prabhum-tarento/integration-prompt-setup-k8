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
/// Archives every entry as JSON to **two** cold-tier containers, independently of each other - a write
/// failure to one never prevents the attempt on the other, since each is wrapped in its own
/// <c>try</c>/<c>catch</c>:
/// <list type="bullet">
/// <item>
/// <see cref="BlobStorageOptions.AuditArchiveContainerName"/>, named
/// <c>{ContainerName}/{TimestampUtc:yyyyMMddHHmmssffffff}_{CorrelationId}_{Schema}_{EntityId}__{EntityPartitionKey}_{Operation}_{Guid}.json</c>
/// - a different convention from the general cold-tier request/response audit's date-partitioned
/// <c>{yyyy}/{MM}/{dd}/...</c> shape (integration-resiliency.instructions.md §5), chosen so every field
/// needed to identify an entry is visible directly in the blob name rather than requiring a download.
/// </item>
/// <item>
/// <see cref="BlobStorageOptions.RequestAuditContainerName"/> (the same <c>request-audit</c> container
/// the Kafka consumer's own per-message audit blobs already live in), named
/// <c>{CorrelationId}/Entity/{ContainerName}/{same file name as the audit-archive blob above}.json</c> -
/// the fixed <c>Entity</c> segment is what keeps this Cosmos-mutation copy from colliding with the Kafka
/// consumer's own <c>{correlationId}/{ConsumerHostedServiceName}/{SchemaName}/...</c> blobs already
/// written into this container (integration-resiliency.instructions.md §5). Reusing the exact same file
/// name as the <c>audit-archive</c> copy (including the same <see cref="Guid.NewGuid"/> value) lets the
/// two copies of one logical write be cross-referenced by name alone.
/// </item>
/// </list>
/// <see cref="AuditEntry.EntityPartitionKey"/> can be a composite key containing <c>:</c>
/// (cosmos-db.instructions.md §4), which is replaced with <c>-</c> in the blob name since a raw colon
/// breaks filesystem-mapping tools (blobfuse, Storage Explorer download) even though Blob Storage itself
/// permits it. Each write failure is logged and swallowed, never rethrown - the audit trail is a
/// diagnostic aid, not the durability boundary (integration-resiliency.instructions.md §5), so a
/// cold-storage outage must not disrupt the rest of <see cref="AuditBackgroundService"/>'s drain loop or
/// any other registered <see cref="IAuditSink"/>. Registered only when
/// <see cref="AuditOptions.ColdStorageEnabled"/> is <see langword="true"/> (see
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
        var json = JsonConvert.SerializeObject(entry);
        var sanitizedEntityPartitionKey = entry.EntityPartitionKey.Replace(':', '-');
        var fileName = $"{entry.TimestampUtc:yyyyMMddHHmmssffffff}_{entry.CorrelationId}_{entry.Schema}_{entry.EntityId}__{sanitizedEntityPartitionKey}_{entry.Operation}_{Guid.NewGuid()}.json";

        await UploadAsync(
            blobStorageOptions.Value.AuditArchiveContainerName,
            $"{entry.ContainerName}/{fileName}",
            json, entry, cancellationToken);

        await UploadAsync(
            blobStorageOptions.Value.RequestAuditContainerName,
            $"{entry.CorrelationId}/Entity/{entry.ContainerName}/{fileName}",
            json, entry, cancellationToken);
    }

    /// <summary>
    /// Uploads <paramref name="json"/> to one destination container, logging and swallowing any
    /// failure - never rethrown, so a failure writing to one container never prevents the attempt on
    /// the other (see this class's remarks).
    /// </summary>
    private async Task UploadAsync(
        string containerName, string blobName, string json, AuditEntry entry, CancellationToken cancellationToken)
    {
        try
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await coldFileStore.UploadAsync(containerName, blobName, stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to archive audit entry {Id} for {ContainerName}/{EntityId} (operation {Operation}) to cold-tier Blob Storage container {DestinationContainer} - continuing without it.",
                entry.Id, entry.ContainerName, entry.EntityId, entry.Operation, containerName);
        }
    }
}
