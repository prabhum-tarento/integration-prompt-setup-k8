using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IInventoryEventService"/>
public sealed class InventoryEventService(
    IInventoryEventRepository repository,
    IDomainEventDispatcher domainEventDispatcher,
    TimeProvider timeProvider,
    ILogger<InventoryEventService> logger) : IInventoryEventService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    /// <inheritdoc />
    public async Task<InventoryEventResponse?> GetAsync(
        string warehouseId, string sku, CancellationToken cancellationToken = default)
    {
        var category = $"{warehouseId}:{sku}";
        logger.LogDebug("Looking up inventory event {Category}.", category);

        var aggregate = await repository.GetAsync(category, category, cancellationToken);

        if (aggregate is null)
        {
            logger.LogDebug("No inventory event found for {Category}.", category);

            return null;
        }

        return ToResponse(aggregate);
    }

    /// <inheritdoc />
    public async Task<InventoryEventResponse> CreateAsync(
        CreateInventoryEventRequest request, CancellationToken cancellationToken = default)
    {
        var id = $"{request.WarehouseId}:{request.Sku}";
        logger.LogDebug(
            "Creating inventory event {Id} with initial quantity {InitialQuantity}.", id, request.InitialQuantity);

        var now = timeProvider.GetUtcNow().UtcDateTime;

        var aggregate = InventoryEvent.Create(id, request.WarehouseId, request.Sku, request.InitialQuantity, now);

        // Repository.CreateAsync treats a duplicate id as "already applied" and returns the
        // existing item rather than throwing - see cosmos-db.instructions.md §5. That makes this
        // call idempotent under Kafka/Service Bus redelivery without any special-casing here.
        var created = await repository.CreateAsync(aggregate, cancellationToken);

        logger.LogInformation(
            "Inventory event {Id} created with on-hand quantity {OnHandQuantity}.", created.Id, created.OnHandQuantity);

        return ToResponse(created);
    }

    /// <inheritdoc />
    public async Task<InventoryEventResponse> ReserveStockAsync(
        string warehouseId, string sku, ReserveStockRequest request, CancellationToken cancellationToken = default)
    {
        var category = $"{warehouseId}:{sku}";

        // Re-read-and-reapply loop: a 412 PreconditionFailed means another writer updated this
        // aggregate between our read and our write. Re-fetch and reapply against the fresh ETag
        // rather than treating it as fatal (integration-resiliency.instructions.md §2). This is a
        // defensive backstop, not the primary correctness mechanism - the Service Bus consumer's
        // session-scoped ordering (§2 of that doc) is what makes true races rare.
        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(category, category, cancellationToken)
                ?? throw new NotFoundException(nameof(InventoryEvent), category);

            aggregate.Reserve(request.ReservationId, request.Quantity, timeProvider.GetUtcNow().UtcDateTime);

            try
            {
                var replaced = await repository.ReplaceAsync(aggregate, aggregate.ETag!, cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                logger.LogInformation(
                    "Reserved {Quantity} unit(s) of {Category} under reservation {ReservationId}.",
                    request.Quantity, category, request.ReservationId);

                return ToResponse(replaced);
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "Concurrency conflict reserving stock for {Category}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    category, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(category, "unknown");
    }

    /// <summary>Maps the Domain aggregate to the Application-facing DTO.</summary>
    private static InventoryEventResponse ToResponse(InventoryEvent aggregate) => new(
        aggregate.Id, aggregate.WarehouseId, aggregate.Sku, aggregate.OnHandQuantity,
        aggregate.CreatedUtc, aggregate.ModifiedUtc);
}
