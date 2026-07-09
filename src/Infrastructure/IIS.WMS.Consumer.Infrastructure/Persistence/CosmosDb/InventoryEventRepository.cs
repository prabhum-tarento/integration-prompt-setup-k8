using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <inheritdoc cref="IInventoryEventRepository"/>
public sealed class InventoryEventRepository : CosmosRepository<InventoryEvent, InventoryEventDocument>, IInventoryEventRepository
{
    /// <summary>
    /// Container this repository reads/writes, declared here rather than in shared configuration
    /// (cosmos-db.instructions.md §1) - every other repository declares its own container name the same way.
    /// </summary>
    private const string ContainerName = "InventoryEvents";

    public InventoryEventRepository(ICosmosContainerFactory containerFactory, ILogger<InventoryEventRepository> logger)
        : base(ContainerName, containerFactory, logger)
    {
    }

    /// <inheritdoc />
    protected override InventoryEventDocument ToDocument(InventoryEvent domain) => InventoryEventMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override InventoryEvent ToDomain(InventoryEventDocument document) => InventoryEventMapper.ToDomain(document);
}
