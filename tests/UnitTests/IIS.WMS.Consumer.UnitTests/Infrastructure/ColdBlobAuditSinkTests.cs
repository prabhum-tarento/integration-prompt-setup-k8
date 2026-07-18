using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ColdBlobAuditSink"/> - archiving every entry to both the
/// <c>audit-archive</c> and <c>request-audit</c> cold-tier containers independently, and swallowing
/// (never rethrowing) an upload failure to either since the audit trail is a diagnostic aid, not the
/// durability boundary (integration-resiliency.instructions.md §5).
/// </summary>
public class ColdBlobAuditSinkTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 34, 56, DateTimeKind.Utc);

    [Fact(DisplayName = "PersistAsync uploads the entry to both the archive and request-audit containers")]
    public async Task PersistAsync_UploadsSucceed_WritesToBothContainers()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/entry.json"));
        var sink = CreateSink(coldFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        var sanitizedPartitionKey = entry.EntityPartitionKey.Replace(':', '-');
        var expectedFileNamePrefix = $"{entry.TimestampUtc:yyyyMMddHHmmssffffff}_{entry.CorrelationId}_{entry.Schema}_{entry.EntityId}__{sanitizedPartitionKey}_{entry.Operation}_";

        await coldFileStore.Received(1).UploadAsync(
            "audit-archive",
            Arg.Is<string>(name => name.StartsWith($"{entry.ContainerName}/{expectedFileNamePrefix}", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());

        await coldFileStore.Received(1).UploadAsync(
            "request-audit",
            Arg.Is<string>(name => name.StartsWith($"{entry.CorrelationId}/Entity/{entry.ContainerName}/{expectedFileNamePrefix}", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws when the audit-archive upload fails")]
    public async Task PersistAsync_ArchiveUploadFails_DoesNotThrow()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync("audit-archive", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        coldFileStore.UploadAsync("request-audit", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/entry.json"));
        var sink = CreateSink(coldFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
        await coldFileStore.Received(1).UploadAsync(
            "request-audit", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws when the request-audit upload fails")]
    public async Task PersistAsync_RequestAuditUploadFails_DoesNotThrow()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync("audit-archive", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/entry.json"));
        coldFileStore.UploadAsync("request-audit", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        var sink = CreateSink(coldFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
        await coldFileStore.Received(1).UploadAsync(
            "audit-archive", Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws when both uploads fail")]
    public async Task PersistAsync_BothUploadsFail_DoesNotThrow()
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
