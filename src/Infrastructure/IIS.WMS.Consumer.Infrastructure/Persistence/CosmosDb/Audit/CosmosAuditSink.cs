using System.Text;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <inheritdoc cref="IAuditSink"/>
/// <remarks>
/// Persists to the Cosmos <c>AuditLog</c> container via <see cref="IAuditRepository"/>. If that write
/// fails even after the Cosmos SDK's own retry policy is exhausted, falls back to the hot-tier
/// <see cref="BlobStorageOptions.AuditDeadLetterContainerName"/> container rather than dropping the
/// entry - only if that fallback write also fails is the entry actually lost. Registered only when
/// <see cref="AuditOptions.CosmosDbEnabled"/> is <see langword="true"/> (see
/// <c>AuditServiceCollectionExtensions.AddAuditTrail</c>).
/// </remarks>
public sealed class CosmosAuditSink(
    IAuditRepository auditRepository,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore deadLetterFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ILogger<CosmosAuditSink> logger) : IAuditSink
{
    /// <inheritdoc />
    public async Task PersistAsync(AuditEntry entry, CancellationToken cancellationToken = default)
    {
        try
        {
            await auditRepository.CreateAsync(entry, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                ex,
                "Failed to persist audit entry {Id} for {ContainerName}/{EntityId} (operation {Operation}) to Cosmos - falling back to hot-tier dead-letter.",
                entry.Id, entry.ContainerName, entry.EntityId, entry.Operation);

            await DeadLetterAsync(entry, cancellationToken);
        }
    }

    /// <summary>Last-resort durability net for an entry whose Cosmos write failed - logged Critical either way, but never silently dropped without at least attempting this.</summary>
    private async Task DeadLetterAsync(AuditEntry entry, CancellationToken cancellationToken)
    {
        try
        {
            var json = JsonConvert.SerializeObject(entry);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await deadLetterFileStore.UploadAsync(
                blobStorageOptions.Value.AuditDeadLetterContainerName, $"{entry.Id}.json", stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                ex,
                "Failed to dead-letter audit entry {Id} to Blob Storage after its Cosmos write also failed - this audit record is lost.",
                entry.Id);
        }
    }
}
