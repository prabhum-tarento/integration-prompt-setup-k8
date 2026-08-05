using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <summary>
/// Raw Cosmos-backed reader for the <see cref="EcomCustomer"/> Ecom-lookup reference record (Q2),
/// shared <c>MasterData</c> container, mirroring <see cref="ItemRepository"/>'s single-container
/// shape. Not registered directly as <see cref="IEcomCustomerRepository"/> - wrapped by
/// <see cref="Caching.CachedEcomCustomerRepository"/> so this reference data isn't re-fetched from
/// Cosmos on every message.
/// </summary>
public sealed class EcomCustomerRepository : CosmosRepository<EcomCustomer, EcomCustomerDocument>
{
    public EcomCustomerRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<EcomCustomerRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.MasterData, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <summary>Reads the Ecom-lookup reference record for a fulfilment code, or <see langword="null"/> if none is configured.</summary>
    public Task<EcomCustomer?> GetByFulfilmentIdAsync(string fulfilmentId, CancellationToken cancellationToken = default)
    {
        var id = EcomCustomer.BuildId(fulfilmentId);
        return GetAsync(id, id, cancellationToken);
    }

    /// <inheritdoc />
    protected override EcomCustomerDocument ToDocument(EcomCustomer domain) => EcomCustomerMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override EcomCustomer ToDomain(EcomCustomerDocument document) => EcomCustomerMapper.ToDomain(document);
}
