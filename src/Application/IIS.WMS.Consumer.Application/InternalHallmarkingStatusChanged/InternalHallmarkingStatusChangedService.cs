using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InternalHallmarkingStatusChanged;

/// <inheritdoc cref="IInternalHallmarkingStatusChangedService"/>
public sealed class InternalHallmarkingStatusChangedService(
    IItemStockInventoryRepository inventoryRepository,
    IItemStockIntransitRepository intransitRepository,
    IItemStockInventorySegmentationService segmentationService,
    IDomainEventDispatcher domainEventDispatcher,
    TimeProvider timeProvider,
    ILogger<InternalHallmarkingStatusChangedService> logger) : IInternalHallmarkingStatusChangedService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    /// <summary>
    /// Local literal for the §5.2 composite key's <c>OrderType</c> segment - kept as a string rather
    /// than referencing the Infrastructure-owned <c>OrderType</c> enum, since the Application layer
    /// must not depend on Infrastructure types (dotnet-architecture-good-practices.instructions.md),
    /// mirroring <see cref="ItemStockInventorySegmentationService"/>'s own <c>TdcFulfilmentId</c> literal.
    /// </summary>
    private const string InternalHallmarkingOrderType = "INTERNALHALLMARKING";

    private const string TransitStatusAllocated = "ALLOCATED";
    private const string TransitStatusPicked = "PICKED";
    private const string TransitStatusIntransit = "INTRANSIT";
    private const string TransitStatusCreated = "CREATED";

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> AllocateAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var deltaResult = new ItemStockInventoryDeltaResult();

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await inventoryRepository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                var exception = new MissingItemStockInventoryException(id, itemCode);
                logger.LogWarning(
                    exception,
                    "MISSING_INVENTORY: no ItemStockInventory record found for {Id} (item {ItemCode}) - skipping STARTED allocation.",
                    id, itemCode);

                return deltaResult;
            }

            var prevB2BAllocated = aggregate.B2BAllocated;
            var prevB2CExtended = aggregate.B2CExtended;
            var prevB2CAvailable = aggregate.B2CAvailable;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            try
            {
                aggregate.AllocateInternalHallmarking(quantity, nowUtc);
            }
            catch (InvalidItemStockInventoryQtyException ex)
            {
                logger.LogWarning(ex, "INVALID_QUANTITY: rejecting STARTED allocation for {Id}.", id);

                return deltaResult;
            }

            List<PatchOperation> operations =
            [
                PatchOperation.Increment("/B2BAllocated", aggregate.B2BAllocated - prevB2BAllocated),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ];

            if (aggregate.IsExtended)
            {
                operations.Add(PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended));
                operations.Add(PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable));
            }

            try
            {
                await inventoryRepository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!, operations, cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                if (aggregate.B2CAvailable != prevB2CAvailable)
                {
                    deltaResult.IsB2CChanged = true;
                    deltaResult.DeltaTowardsOms = aggregate.B2CAvailable - prevB2CAvailable;
                }

                await UpsertIntransitLegAsync(
                    itemCode, hallmark, countryOfOrigin, fulfilmentId, TransitStatusAllocated, quantity, cancellationToken);

                logger.LogInformation(
                    "Applied STARTED allocation to ItemStockInventory {Id}: quantity={Quantity}.", id, quantity);

                return deltaResult;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "Concurrency conflict applying STARTED allocation to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> PickAndShipAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var deltaResult = new ItemStockInventoryDeltaResult();

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await inventoryRepository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                var exception = new MissingItemStockInventoryException(id, itemCode);
                logger.LogWarning(
                    exception,
                    "MISSING_INVENTORY: no ItemStockInventory record found for {Id} (item {ItemCode}) - skipping PICKED processing.",
                    id, itemCode);

                return deltaResult;
            }

            var prevB2BAllocated = aggregate.B2BAllocated;
            var prevB2BPrepared = aggregate.B2BPrepared;
            var prevB2BAvailable = aggregate.B2BAvailable;
            var prevPsc = aggregate.Psc;
            var prevB2BUsedShare = aggregate.B2BUsedShare;
            var prevB2CExtended = aggregate.B2CExtended;
            var prevB2CAvailable = aggregate.B2CAvailable;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            aggregate.PickB2B(quantity, nowUtc);
            aggregate.ApplyConsolidatedShipment(ConfirmationType.STANDARD, quantity, nowUtc);

            List<PatchOperation> operations =
            [
                PatchOperation.Increment("/B2BAllocated", aggregate.B2BAllocated - prevB2BAllocated),
                PatchOperation.Increment("/B2BPrepared", aggregate.B2BPrepared - prevB2BPrepared),
                PatchOperation.Increment("/B2BAVL", aggregate.B2BAvailable - prevB2BAvailable),
                PatchOperation.Increment("/PSC", aggregate.Psc - prevPsc),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ];

            if (aggregate.IsExtended)
            {
                operations.Add(PatchOperation.Increment("/B2BUsedShare", aggregate.B2BUsedShare - prevB2BUsedShare));
                operations.Add(PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended));
                operations.Add(PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable));
            }

            try
            {
                await inventoryRepository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!, operations, cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                if (aggregate.B2CAvailable != prevB2CAvailable)
                {
                    deltaResult.IsB2CChanged = true;
                    deltaResult.DeltaTowardsOms = aggregate.B2CAvailable - prevB2CAvailable;
                }

                await UpsertIntransitLegAsync(
                    itemCode, hallmark, countryOfOrigin, fulfilmentId, TransitStatusPicked, quantity, cancellationToken);
                await UpsertIntransitLegAsync(
                    itemCode, hallmark, countryOfOrigin, fulfilmentId, TransitStatusAllocated, -quantity, cancellationToken);

                logger.LogInformation(
                    "Applied PICKED pick-and-ship to ItemStockInventory {Id}: quantity={Quantity}.", id, quantity);

                return deltaResult;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "Concurrency conflict applying PICKED processing to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> ChangeHallmarkAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmarkFrom, string hallmarkTo,
        int quantity, bool isThirdPartyLogistics, CancellationToken cancellationToken = default)
    {
        var deltaToResult = await segmentationService.ApplySegmentationAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmarkTo, quantity, isThirdPartyLogistics, cancellationToken);

        var deltaFromResult = await segmentationService.ApplySegmentationAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmarkFrom, -quantity, isThirdPartyLogistics, cancellationToken);

        await UpsertIntransitLegAsync(
            itemCode, hallmarkTo, countryOfOrigin, fulfilmentId, TransitStatusIntransit, quantity, cancellationToken);
        await UpsertIntransitLegAsync(
            itemCode, hallmarkFrom, countryOfOrigin, fulfilmentId, TransitStatusIntransit, -quantity, cancellationToken);

        return new ItemStockInventoryDeltaResult
        {
            IsB2CChanged = deltaToResult.IsB2CChanged || deltaFromResult.IsB2CChanged,
            DeltaTowardsOms = deltaToResult.DeltaTowardsOms + deltaFromResult.DeltaTowardsOms,
        };
    }

    /// <inheritdoc />
    public async Task CompleteTransitAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await inventoryRepository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                var exception = new MissingItemStockInventoryException(id, itemCode);
                logger.LogWarning(
                    exception,
                    "MISSING_INVENTORY: no ItemStockInventory record found for {Id} (item {ItemCode}) - skipping FINISHED transit completion.",
                    id, itemCode);

                return;
            }

            var prevInTransit = aggregate.InTransit;
            var prevB2BAvailable = aggregate.B2BAvailable;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            try
            {
                aggregate.CompleteInternalHallmarkingTransit(quantity, nowUtc);
            }
            catch (InvalidItemStockInventoryQtyException ex)
            {
                logger.LogWarning(ex, "INVALID_QUANTITY: rejecting FINISHED transit completion for {Id}.", id);

                return;
            }

            List<PatchOperation> operations =
            [
                PatchOperation.Increment("/InTransit", aggregate.InTransit - prevInTransit),
                PatchOperation.Increment("/B2BAVL", aggregate.B2BAvailable - prevB2BAvailable),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ];

            try
            {
                await inventoryRepository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!, operations, cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                await UpsertIntransitLegAsync(
                    itemCode, hallmark, countryOfOrigin, fulfilmentId, TransitStatusIntransit, -quantity, cancellationToken);
                await UpsertIntransitLegAsync(
                    itemCode, hallmark, countryOfOrigin, fulfilmentId, TransitStatusCreated, quantity, cancellationToken);

                logger.LogInformation(
                    "Applied FINISHED transit completion to ItemStockInventory {Id}: quantity={Quantity}.", id, quantity);

                return;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "Concurrency conflict applying FINISHED transit completion to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }

    /// <summary>
    /// §3.5/§5.2 <c>manageIntransitAsync</c> in-transit bookkeeping - creates the status leg if it
    /// doesn't exist yet (mirroring <see cref="ItemStockInventorySegmentationService"/>'s
    /// create-if-missing pattern), then applies the signed <paramref name="delta"/> via
    /// <see cref="ItemStockIntransit.IncreaseQuantity"/>/<see cref="ItemStockIntransit.DecreaseQuantity"/>.
    /// A no-op if <paramref name="delta"/> is zero. <see cref="InvalidItemStockInventoryQtyException"/>
    /// from a decrease that would take the leg negative is a business rejection (logged, skipped) per
    /// §6's "in-transit never decremented below zero" invariant - not a poison message.
    /// </summary>
    private async Task UpsertIntransitLegAsync(
        string itemCode, string hallmarkCode, string countryOfOrigin, string fulfilmentCode, string status,
        int delta, CancellationToken cancellationToken)
    {
        if (delta == 0)
        {
            return;
        }

        var id = ItemStockIntransit.BuildId(
            itemCode, hallmarkCode, countryOfOrigin, InternalHallmarkingOrderType, fulfilmentCode, status);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var entity = await intransitRepository.GetAsync(id, id, cancellationToken);
            var wasCreated = entity is null;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            entity ??= ItemStockIntransit.CreateDefault(
                itemCode, hallmarkCode, countryOfOrigin, InternalHallmarkingOrderType, fulfilmentCode, status, nowUtc);

            try
            {
                if (delta > 0)
                {
                    entity.IncreaseQuantity(delta, nowUtc);
                }
                else
                {
                    entity.DecreaseQuantity(-delta, nowUtc);
                }
            }
            catch (InvalidItemStockInventoryQtyException ex)
            {
                logger.LogWarning(ex, "INVALID_QUANTITY: rejecting in-transit {Status} leg update for {Id}.", status, id);

                return;
            }

            try
            {
                if (wasCreated)
                {
                    await intransitRepository.CreateAsync(entity, cancellationToken);
                }
                else
                {
                    await intransitRepository.PatchAsync(
                        entity.Id, entity.Category, entity.ETag!,
                        [
                            PatchOperation.Increment("/Quantity", delta),
                            PatchOperation.Set("/ModifiedUtc", entity.ModifiedUtc.ToString("O")),
                        ],
                        cancellationToken);
                }

                await domainEventDispatcher.DispatchAsync(entity.DomainEvents, cancellationToken);

                return;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    "Concurrency conflict applying in-transit {Status} leg update for {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    status, id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }
}
