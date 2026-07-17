using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IBulkInventoryImportRepository"/>
/// <remarks>
/// Not <see langword="sealed"/>: an integration-test-only subclass overrides
/// <see cref="CosmosRepository{TDomain,TDocument}.ReadNextPageAsync{T}"/> to run the paged/projected query
/// methods against an in-memory <see cref="ICosmosContainerFactory"/> fake (integration-resiliency.instructions.md
/// §9) - production behavior is unaffected, since that method's default implementation is unchanged.
/// </remarks>
public class InventoryBulkImportItemRepository : CosmosRepository<InventoryBulkImportItem, InventoryBulkImportItemDocument>, IBulkInventoryImportRepository
{
    /// <summary>
    /// Container this repository reads/writes, declared here rather than in shared configuration
    /// (cosmos-db.instructions.md §1) - every other repository declares its own container name the same way.
    /// Provisioned externally (Bicep/Terraform) like every other container - not created by this app.
    /// </summary>
    private const string ContainerName = "BulkInventoryImports";

    /// <param name="containerFactory">
    /// The <see cref="CosmosDbServiceCollectionExtensions.BulkCosmosClientKey"/>-keyed container
    /// factory, backed by a separate <c>CosmosClient</c> with <c>AllowBulkExecution = true</c> - not
    /// the plain factory <c>InventoryEventRepository</c> uses. Bulk mode optimizes throughput at the
    /// cost of per-call latency, which would be wrong to apply to the latency-sensitive
    /// reserve/allocate path, so the two repositories deliberately use different Cosmos clients.
    /// </param>
    public InventoryBulkImportItemRepository(
        [FromKeyedServices(CosmosDbServiceCollectionExtensions.BulkCosmosClientKey)] ICosmosContainerFactory containerFactory,
        ILogger<InventoryBulkImportItemRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(ContainerName, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override InventoryBulkImportItemDocument ToDocument(InventoryBulkImportItem domain) => InventoryBulkImportItemMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override InventoryBulkImportItem ToDomain(InventoryBulkImportItemDocument document) => InventoryBulkImportItemMapper.ToDomain(document);
}
