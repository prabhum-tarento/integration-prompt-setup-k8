using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <inheritdoc cref="IBulkInventoryImportRepository"/>
public sealed class InventoryBulkImportItemRepository : CosmosRepository<InventoryBulkImportItem, InventoryBulkImportItemDocument>, IBulkInventoryImportRepository
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
        ILogger<InventoryBulkImportItemRepository> logger)
        : base(ContainerName, containerFactory, logger)
    {
    }

    /// <inheritdoc />
    protected override InventoryBulkImportItemDocument ToDocument(InventoryBulkImportItem domain) => InventoryBulkImportItemMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override InventoryBulkImportItem ToDomain(InventoryBulkImportItemDocument document) => InventoryBulkImportItemMapper.ToDomain(document);
}
