using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="NullAuditTrailWriter"/> - the no-op <see cref="IAuditTrailWriter"/>
/// used only to break <c>AuditRepository</c>'s own audit-write recursion (see
/// <see cref="AuditServiceCollectionExtensions"/>'s remarks).
/// </summary>
public class NullAuditTrailWriterTests
{
    [Fact(DisplayName = "Instance returns the same singleton every time")]
    public void Instance_CalledRepeatedly_ReturnsSameSingleton()
    {
        Assert.Same(NullAuditTrailWriter.Instance, NullAuditTrailWriter.Instance);
    }

    [Fact(DisplayName = "Enqueue does nothing and never throws")]
    public void Enqueue_AnyEntry_DoesNothing()
    {
        var entry = AuditEntry.Create(
            id: "1:guid",
            containerName: "InventoryEvents",
            entityId: "WH1:SKU1",
            entityPartitionKey: "WH1:SKU1",
            operation: AuditOperation.Create,
            correlationId: "corr-1",
            schema: "InventoryStateChanged",
            documentJson: "{}",
            timestampUtc: DateTime.UtcNow);

        var exception = Record.Exception(() => NullAuditTrailWriter.Instance.Enqueue(entry));

        Assert.Null(exception);
    }
}
