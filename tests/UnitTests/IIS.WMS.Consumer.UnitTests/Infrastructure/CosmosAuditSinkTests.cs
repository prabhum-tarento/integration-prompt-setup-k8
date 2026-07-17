using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="CosmosAuditSink"/> - persisting to the Cosmos <c>AuditLog</c>
/// container via <see cref="IAuditRepository"/>, and falling back to the hot-tier dead-letter blob
/// container when that write fails.
/// </summary>
public class CosmosAuditSinkTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "PersistAsync writes the entry through IAuditRepository")]
    public async Task PersistAsync_RepositorySucceeds_WritesThroughRepositoryOnly()
    {
        var auditRepository = Substitute.For<IAuditRepository>();
        auditRepository.CreateAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(callInfo.Arg<AuditEntry>()));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        var sink = CreateSink(auditRepository, deadLetterFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        await auditRepository.Received(1).CreateAsync(Arg.Is<AuditEntry>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        await deadLetterFileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "PersistAsync falls back to the hot-tier dead-letter blob when the Cosmos write fails")]
    public async Task PersistAsync_RepositoryFails_FallsBackToDeadLetterBlob()
    {
        var auditRepository = Substitute.For<IAuditRepository>();
        auditRepository.CreateAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuditEntry>>(_ => throw new InvalidOperationException("Cosmos write failed"));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        deadLetterFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("https://blob/audit-dead-letter/entry.json"));
        var sink = CreateSink(auditRepository, deadLetterFileStore);

        var entry = CreateEntry();
        await sink.PersistAsync(entry);

        await deadLetterFileStore.Received(1).UploadAsync(
            "audit-dead-letter", $"{entry.Id}.json", Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PersistAsync never throws even when both the Cosmos write and the dead-letter fallback fail")]
    public async Task PersistAsync_RepositoryAndDeadLetterBothFail_DoesNotThrow()
    {
        var auditRepository = Substitute.For<IAuditRepository>();
        auditRepository.CreateAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<AuditEntry>>(_ => throw new InvalidOperationException("Cosmos write failed"));
        var deadLetterFileStore = Substitute.For<IFileStore>();
        deadLetterFileStore.UploadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("Blob upload failed"));
        var sink = CreateSink(auditRepository, deadLetterFileStore);

        var exception = await Record.ExceptionAsync(() => sink.PersistAsync(CreateEntry()));

        Assert.Null(exception);
    }

    private static CosmosAuditSink CreateSink(IAuditRepository auditRepository, IFileStore deadLetterFileStore)
    {
        var blobStorageOptions = Options.Create(new BlobStorageOptions());

        return new CosmosAuditSink(
            auditRepository,
            deadLetterFileStore,
            blobStorageOptions,
            Substitute.For<ILogger<CosmosAuditSink>>());
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
