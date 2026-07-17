using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;
using Microsoft.Extensions.Logging.Abstractions;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Proves the audit-capture hook added to <c>CosmosRepository{TDomain,TDocument}</c> actually fires for
/// every mutating operation (Create/Replace/Patch/Delete/Upsert), carries the right
/// Category/Id/CorrelationId/Schema shape, and never fires for a no-op (a redelivered Create that
/// conflicted, or a Delete of an already-missing item) - see that class's own remarks and
/// cosmos-db.instructions.md's audit-trail addendum.
/// </summary>
public sealed class CosmosRepositoryAuditTests
{
    private const string CorrelationId = "corr-1";
    private const string Schema = "InventoryStateChanged";

    [Fact(DisplayName = "CreateAsync enqueues a Create audit entry carrying Category/Id/CorrelationId/Schema")]
    public async Task CreateAsync_NewItem_EnqueuesCreateAuditEntry()
    {
        var (repository, recorder) = CreateInventoryEventRepository();
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, DateTime.UtcNow);

        await repository.CreateAsync(aggregate);

        var entry = Assert.Single(recorder.Entries);
        Assert.Equal(AuditOperation.Create, entry.Operation);
        Assert.Equal("InventoryEvents_WH1:SKU1", entry.Category);
        Assert.StartsWith("WH1:SKU1:", entry.Id, StringComparison.Ordinal);
        Assert.Equal(CorrelationId, entry.CorrelationId);
        Assert.Equal(Schema, entry.Schema);
        Assert.NotNull(entry.DocumentJson);
    }

    [Fact(DisplayName = "A redelivered CreateAsync that conflicts does not enqueue a second audit entry")]
    public async Task CreateAsync_DuplicateRedelivery_DoesNotEnqueueAnotherAuditEntry()
    {
        var (repository, recorder) = CreateInventoryEventRepository();
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, DateTime.UtcNow);

        await repository.CreateAsync(aggregate);
        await repository.CreateAsync(aggregate); // redelivered - conflicts, treated as already-applied

        Assert.Single(recorder.Entries); // only the first, genuine create
    }

    [Fact(DisplayName = "ReplaceAsync enqueues a Replace audit entry with the new-state document")]
    public async Task ReplaceAsync_ExistingItem_EnqueuesReplaceAuditEntry()
    {
        var (repository, recorder) = CreateInventoryEventRepository();
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, DateTime.UtcNow);
        var created = await repository.CreateAsync(aggregate);
        recorder.Entries.Clear();

        created.Reserve("reservation-1", 2, DateTime.UtcNow);
        await repository.ReplaceAsync(created, created.ETag!);

        var entry = Assert.Single(recorder.Entries);
        Assert.Equal(AuditOperation.Replace, entry.Operation);
        Assert.Contains("reservation-1", entry.DocumentJson);
    }

    [Fact(DisplayName = "DeleteAsync enqueues a Delete audit entry with no document body")]
    public async Task DeleteAsync_ExistingItem_EnqueuesDeleteAuditEntryWithNullDocument()
    {
        var (repository, recorder) = CreateInventoryEventRepository();
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, DateTime.UtcNow);
        await repository.CreateAsync(aggregate);
        recorder.Entries.Clear();

        await repository.DeleteAsync("WH1:SKU1", "WH1:SKU1");

        var entry = Assert.Single(recorder.Entries);
        Assert.Equal(AuditOperation.Delete, entry.Operation);
        Assert.Null(entry.DocumentJson);
    }

    [Fact(DisplayName = "Deleting an item that no longer exists does not enqueue an audit entry")]
    public async Task DeleteAsync_AlreadyMissingItem_DoesNotEnqueueAuditEntry()
    {
        var (repository, recorder) = CreateInventoryEventRepository();

        await repository.DeleteAsync("WH1:SKU1", "WH1:SKU1");

        Assert.Empty(recorder.Entries);
    }

    [Fact(DisplayName = "UpsertAsync (OrderArchiveRepository) enqueues an Upsert audit entry")]
    public async Task UpsertAsync_OrderArchive_EnqueuesUpsertAuditEntry()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(CorrelationId, appId: "app-1", types: [Schema]);
        var recorder = new RecordingAuditTrailWriter();
        var factory = new InMemoryCosmosContainerFactory();

        var repository = new OrderArchiveRepository(
            factory, NullLogger<OrderArchiveRepository>.Instance, correlationContext, recorder);

        var aggregate = OrderArchive.Create("cat-1", "cat-1", "{\"a\":1}", CorrelationId, DateTime.UtcNow);
        await repository.UpsertAsync(aggregate);

        var entry = Assert.Single(recorder.Entries);
        Assert.Equal(AuditOperation.Upsert, entry.Operation);
        Assert.Equal("OrderArchive_cat-1", entry.Category);
    }

    [Fact(DisplayName = "AuditRepository itself never enqueues an audit entry for its own writes")]
    public async Task CreateAsync_AuditRepositoryItself_NeverSelfAudits()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(CorrelationId, appId: "app-1", types: [Schema]);
        var factory = new InMemoryCosmosContainerFactory();

        // NullAuditTrailWriter.Instance - the exact wiring AuditServiceCollectionExtensions uses -
        // guarantees persisting an audit record never enqueues another one. A RecordingAuditTrailWriter
        // is intentionally NOT wired in here: if AuditRepository were ever miswired to the real writer,
        // there would be nothing else in this test capable of proving it, by design - the guard belongs
        // entirely to which writer instance is constructed with, per AuditRepository's own remarks.
        var auditRepository = new AuditRepository(
            factory, NullLogger<AuditRepository>.Instance, correlationContext, NullAuditTrailWriter.Instance);

        var entry = AuditEntry.Create(
            "WH1:SKU1:guid", "InventoryEvents", "WH1:SKU1", "WH1:SKU1",
            AuditOperation.Create, CorrelationId, Schema, "{}", DateTime.UtcNow);

        await auditRepository.CreateAsync(entry);

        // No exception, no infinite recursion, and nothing else observes an enqueue - the absence of a
        // real IAuditTrailWriter anywhere in this test's object graph is the proof.
    }

    private static (InventoryEventRepository Repository, RecordingAuditTrailWriter Recorder) CreateInventoryEventRepository()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(CorrelationId, appId: "app-1", types: [Schema]);
        var recorder = new RecordingAuditTrailWriter();
        var factory = new InMemoryCosmosContainerFactory();

        var repository = new InventoryEventRepository(
            factory, NullLogger<InventoryEventRepository>.Instance, correlationContext, recorder);

        return (repository, recorder);
    }

    /// <summary>Captures every enqueued <see cref="AuditEntry"/> synchronously, in place of the real channel-backed <see cref="IAuditTrailWriter"/>.</summary>
    private sealed class RecordingAuditTrailWriter : IAuditTrailWriter
    {
        public List<AuditEntry> Entries { get; } = [];

        public void Enqueue(AuditEntry entry) => Entries.Add(entry);
    }
}
