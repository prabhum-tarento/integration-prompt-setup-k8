using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="CosmosMessageArchiveSink"/> - persisting to the Cosmos
/// <c>MessageArchive</c> container via <see cref="IMessageArchiveRepository"/>, and falling back to the
/// hot-tier dead-letter blob container when that write fails.
/// </summary>
public class CosmosMessageArchiveSinkTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "PersistAsync writes the entry through IMessageArchiveRepository")]
    public async Task PersistAsync_RepositorySucceeds_WritesThroughRepositoryOnly()
    {
        var messageArchiveRepository = Substitute.For<IMessageArchiveRepository>();
        messageArchiveRepository.UpsertAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(callInfo.Arg<MessageArchive>()));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        var sink = CreateSink(messageArchiveRepository, deadLetterFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        await messageArchiveRepository.Received(1).UpsertAsync(Arg.Is<MessageArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        await deadLetterFileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "PersistAsync falls back to the hot-tier dead-letter blob when the Cosmos write fails")]
    public async Task PersistAsync_RepositoryFails_FallsBackToDeadLetterBlob()
    {
        var messageArchiveRepository = Substitute.For<IMessageArchiveRepository>();
        messageArchiveRepository.UpsertAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>())
            .Returns<Task<MessageArchive>>(_ => throw new InvalidOperationException("Cosmos write failed"));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        deadLetterFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/message-archive-dead-letter/entry.json"));
        var sink = CreateSink(messageArchiveRepository, deadLetterFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        await deadLetterFileStore.Received(1).UploadAsync(
            "message-archive-dead-letter", $"{entry.Id}.json", Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws even when both the Cosmos write and the dead-letter fallback fail")]
    public async Task PersistAsync_RepositoryAndDeadLetterBothFail_DoesNotThrow()
    {
        var messageArchiveRepository = Substitute.For<IMessageArchiveRepository>();
        messageArchiveRepository.UpsertAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>())
            .Returns<Task<MessageArchive>>(_ => throw new InvalidOperationException("Cosmos write failed"));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        deadLetterFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        var sink = CreateSink(messageArchiveRepository, deadLetterFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
    }

    private static CosmosMessageArchiveSink CreateSink(IMessageArchiveRepository messageArchiveRepository, IFileStore deadLetterFileStore)
    {
        var blobStorageOptions = Options.Create(new BlobStorageOptions());

        return new CosmosMessageArchiveSink(
            messageArchiveRepository,
            deadLetterFileStore,
            blobStorageOptions,
            Substitute.For<ILogger<CosmosMessageArchiveSink>>());
    }

    private static MessageArchive CreateEntry() => MessageArchive.Create(
        id: "InventoryStateChanged_corr-1",
        category: "InventoryStateChanged",
        payload: "{}",
        correlationId: "corr-1",
        timestamp: Now);
}
