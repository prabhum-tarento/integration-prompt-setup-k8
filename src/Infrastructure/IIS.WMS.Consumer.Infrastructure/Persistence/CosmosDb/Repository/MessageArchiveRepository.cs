using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IMessageArchiveRepository"/>
public sealed class MessageArchiveRepository : CosmosRepository<MessageArchive, MessageArchiveDocument>, IMessageArchiveRepository
{
    public MessageArchiveRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<MessageArchiveRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.MessageArchive, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override MessageArchiveDocument ToDocument(MessageArchive domain) => MessageArchiveMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override MessageArchive ToDomain(MessageArchiveDocument document) => MessageArchiveMapper.ToDomain(document);
}
