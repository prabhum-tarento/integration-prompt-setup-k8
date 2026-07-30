using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="ICountryRepository"/>
public sealed class CountryRepository : CosmosRepository<CountryMaster, CountryDocument>, ICountryRepository
{
    public CountryRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<CountryRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.MasterData, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    private static string Category(string code) => $"Country_{code}";

    /// <inheritdoc/>
    public Task<CountryMaster?> GetByCodeAsync(string countryCode, CancellationToken cancellationToken = default) =>
        GetAsync(countryCode, Category(countryCode), cancellationToken);

    protected override CountryDocument ToDocument(CountryMaster domain) => CountryMapper.ToDocument(domain);

    protected override CountryMaster ToDomain(CountryDocument document) => CountryMapper.ToDomain(document);
}
