using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ColdBlobAuditSink"/> - archiving every entry to the cold-tier
/// <c>audit-archive</c> container, and swallowing (never rethrowing) an upload failure since the audit
/// trail is a diagnostic aid, not the durability boundary (integration-resiliency.instructions.md §5).
/// </summary>
public class ColdBlobAuditSinkTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 34, 56, DateTimeKind.Utc);

    [Fact(DisplayName = "PersistAsync uploads the entry to the archive container at the date-partitioned path")]
    public async Task PersistAsync_UploadSucceeds_UsesDatePartitionedBlobName()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/audit-archive/entry.json"));
        var sink = CreateSink(coldFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        await coldFileStore.Received(1).UploadAsync(
            "audit-archive", $"2026/07/16/{entry.Id}.json", Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws when the cold-tier upload fails")]
    public async Task PersistAsync_UploadFails_DoesNotThrow()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        var sink = CreateSink(coldFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
    }

    private static ColdBlobAuditSink CreateSink(IFileStore coldFileStore)
    {
        var blobStorageOptions = Options.Create(new BlobStorageOptions());

        return new ColdBlobAuditSink(
            coldFileStore,
            blobStorageOptions,
            Substitute.For<ILogger<ColdBlobAuditSink>>());
    }

    private static AuditEntry CreateEntry() => AuditEntry.Create(
        id: Guid.NewGuid().ToString(),
        containerName: "InventoryEvents",
        entityId: "WH1:SKU1",
        entityPartitionKey: "WH1:SKU1",
        operation: AuditOperation.Create,
        correlationId: "corr-1",
        schema: "InventoryStateChanged",
        documentJson: "{}",
        timestampUtc: Now);
}
