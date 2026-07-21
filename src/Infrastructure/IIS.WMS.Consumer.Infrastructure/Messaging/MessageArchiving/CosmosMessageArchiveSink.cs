using System.Text;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <inheritdoc cref="IMessageArchiveSink"/>
/// <remarks>
/// Persists to the Cosmos <c>MessageArchive</c> container via <see cref="IMessageArchiveRepository"/>. If
/// that write fails even after the Cosmos SDK's own retry policy is exhausted, falls back to the
/// hot-tier <see cref="BlobStorageOptions.MessageArchiveDeadLetterContainerName"/> container rather than
/// dropping the entry - only if that fallback write also fails is the entry actually lost. Registered
/// only when <see cref="MessageArchiveOptions.CosmosDbEnabled"/> is <see langword="true"/> (see
/// <c>MessageArchiveServiceCollectionExtensions.AddMessageArchiving</c>).
/// </remarks>
public sealed class CosmosMessageArchiveSink(
    IMessageArchiveRepository messageArchiveRepository,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore deadLetterFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ILogger<CosmosMessageArchiveSink> logger) : IMessageArchiveSink
{
    /// <inheritdoc />
    public async Task PersistAsync(MessageArchive entry, CancellationToken cancellationToken = default)
    {
        try
        {
            await messageArchiveRepository.UpsertAsync(entry, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                ex,
                "Failed to persist message archive entry {Id}/{Category} to Cosmos - falling back to hot-tier dead-letter.",
                entry.Id, entry.Category);

            await DeadLetterAsync(entry, cancellationToken);
        }
    }

    /// <summary>Last-resort durability net for an entry whose Cosmos write failed - logged Critical either way, but never silently dropped without at least attempting this.</summary>
    private async Task DeadLetterAsync(MessageArchive entry, CancellationToken cancellationToken)
    {
        try
        {
            var json = JsonConvert.SerializeObject(entry);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await deadLetterFileStore.UploadAsync(
                blobStorageOptions.Value.MessageArchiveDeadLetterContainerName, $"{entry.Id}.json", stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                ex,
                "Failed to dead-letter message archive entry {Id} to Blob Storage after its Cosmos write also failed - this archive record is lost.",
                entry.Id);
        }
    }
}
