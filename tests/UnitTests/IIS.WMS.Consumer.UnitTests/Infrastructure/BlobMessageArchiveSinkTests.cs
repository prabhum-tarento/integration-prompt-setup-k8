using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="BlobMessageArchiveSink"/> - archiving every entry to the single
/// cold-tier <c>MessageArchiveContainerName</c> container, and swallowing (never rethrowing) an upload
/// failure since the message archive is a diagnostic aid, not the durability boundary
/// (integration-resiliency.instructions.md §5). Simplified relative to <c>ColdBlobAuditSinkTests</c>
/// since this sink writes to one container, not two.
/// </summary>
public class BlobMessageArchiveSinkTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 34, 56, DateTimeKind.Utc);

    [Fact(DisplayName = "PersistAsync uploads the entry to the message-archive container")]
    public async Task PersistAsync_UploadSucceeds_WritesToContainer()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/entry.json"));
        var sink = CreateSink(coldFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        var expectedFileNamePrefix = $"{entry.Timestamp:yyyyMMddHHmmssffffff}_{entry.CorrelationId}_{entry.Category}_";

        await coldFileStore.Received(1).UploadAsync(
            "message-archive",
            Arg.Is<string>(name => name.StartsWith(expectedFileNamePrefix, StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws when the upload fails")]
    public async Task PersistAsync_UploadFails_DoesNotThrow()
    {
        var coldFileStore = Substitute.For<IFileStore>();
        coldFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        var sink = CreateSink(coldFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
    }

    private static BlobMessageArchiveSink CreateSink(IFileStore coldFileStore)
    {
        var blobStorageOptions = Options.Create(new BlobStorageOptions());

        return new BlobMessageArchiveSink(
            coldFileStore,
            blobStorageOptions,
            Substitute.For<ILogger<BlobMessageArchiveSink>>());
    }

    private static MessageArchive CreateEntry() => MessageArchive.Create(
        id: "InventoryStateChanged_corr-1",
        category: "InventoryStateChanged",
        payload: "{}",
        correlationId: "corr-1",
        timestamp: Now);
}
