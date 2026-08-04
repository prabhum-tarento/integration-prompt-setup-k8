using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IItemStockInventoryService"/>
public sealed class ItemStockInventoryService(
    IItemStockInventoryRepository repository,
    IDomainEventDispatcher domainEventDispatcher,
    TimeProvider timeProvider,
    ILogger<ItemStockInventoryService> logger) : IItemStockInventoryService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    /// <inheritdoc />
    public Task ApplyPickAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        ItemStockPickChannel channel, int quantity, CancellationToken cancellationToken = default) =>
        ApplyAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            aggregate =>
            {
                var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

                if (channel == ItemStockPickChannel.B2B)
                {
                    aggregate.PickB2B(quantity, nowUtc);
                }
                else
                {
                    aggregate.PickB2C(quantity, nowUtc);
                }
            },
            buildPatchOperations: (before, aggregate) => channel == ItemStockPickChannel.B2B
                ? BuildB2BPickPatch(before, aggregate)
                : BuildB2CPickPatch(before, aggregate),
            cancellationToken);

    /// <inheritdoc />
    public Task ApplyUnpickAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default) =>
        ApplyAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            aggregate => aggregate.Unpick(quantity, timeProvider.GetUtcNow().UtcDateTime),
            buildPatchOperations: (before, aggregate) =>
            [
                PatchOperation.Increment("/B2BPrepared", aggregate.B2BPrepared - before.B2BPrepared),
                PatchOperation.Increment("/B2BAllocated", aggregate.B2BAllocated - before.B2BAllocated),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ],
            cancellationToken);

    /// <summary>
    /// The exact field set <see cref="ItemStockInventory.PickB2B"/> mutates: allocated/prepared/modified
    /// always, plus used-share only when extension is active. Emits atomic <c>PatchOperation.Increment</c>
    /// deltas (never a last-write-wins absolute <c>PatchOperation.Set</c>) per
    /// [delta-towards-oms.md](../../../../docs/events/shared/delta-towards-oms.md) and cosmos-db.instructions.md §10.
    /// </summary>
    private static List<PatchOperation> BuildB2BPickPatch(QuantitySnapshot before, ItemStockInventory aggregate)
    {
        List<PatchOperation> operations =
        [
            PatchOperation.Increment("/B2BAllocated", aggregate.B2BAllocated - before.B2BAllocated),
            PatchOperation.Increment("/B2BPrepared", aggregate.B2BPrepared - before.B2BPrepared),
            PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
        ];

        if (aggregate.IsExtended)
        {
            operations.Add(PatchOperation.Increment("/B2BUsedShare", aggregate.B2BUsedShare - before.B2BUsedShare));
        }

        return operations;
    }

    /// <summary>
    /// The exact field set <see cref="ItemStockInventory.PickB2C"/> mutates: prepared always, allocated
    /// always, modified always, and used-share only on the extended-oversell branch. Emits atomic
    /// <c>PatchOperation.Increment</c> deltas - see <see cref="BuildB2BPickPatch"/>.
    /// </summary>
    private static IReadOnlyList<PatchOperation> BuildB2CPickPatch(QuantitySnapshot before, ItemStockInventory aggregate) =>
    [
        PatchOperation.Increment("/B2CPrepared", aggregate.B2CPrepared - before.B2CPrepared),
        PatchOperation.Increment("/B2CAllocated", aggregate.B2CAllocated - before.B2CAllocated),
        PatchOperation.Increment("/B2BUsedShare", aggregate.B2BUsedShare - before.B2BUsedShare),
        PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
    ];

    /// <inheritdoc />
    public async Task<ItemStockSyncApplyResult> ApplyStockSyncAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int b2cAvl, int b2cPrepared, int? b2cAvailableToSell, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(id, id, cancellationToken);
            var wasCreated = aggregate is null;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            aggregate ??= ItemStockInventory.CreateDefault(fulfilmentId, itemCode, hallmark, countryOfOrigin, nowUtc);

            var previousB2CAvailable = aggregate.B2CAvailable;

            aggregate.ApplyStockSync(b2cAvl, b2cPrepared, b2cAvailableToSell, nowUtc);

            try
            {
                if (wasCreated)
                {
                    await repository.CreateAsync(aggregate, cancellationToken);
                }
                else
                {
                    await repository.PatchAsync(
                        aggregate.Id, aggregate.Category, aggregate.ETag!, BuildStockSyncPatch(aggregate), cancellationToken);
                }

                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                logger.LogInformation(
                    "Applied §3.2 stock sync to ItemStockInventory {Id}: B2CAVL={B2CAvl}, B2CPrepared={B2CPrepared}.",
                    id, b2cAvl, b2cPrepared);

                return new ItemStockSyncApplyResult
                {
                    PreviousB2CAvailable = previousB2CAvailable,
                    NewB2CAvailable = aggregate.B2CAvailable,
                    WasCreated = wasCreated,
                };
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    "Concurrency conflict applying stock sync to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }

    /// <summary>
    /// The exact field set §3.2/§5.1 (docs/events/inventory.StockSyncSubmitted.md) Sets on a stock
    /// sync: <c>B2CAVL</c>/<c>B2CPrepared</c> always, <c>B2CAvailableToSell</c> only when this sync
    /// reported it (BR-only state) - never a last-write-wins field omitted from the operation list,
    /// but also never Set when the aggregate's value is <see langword="null"/> (nothing to overwrite).
    /// Unlike every other patch builder on this service, these are <c>PatchOperation.Set</c>, not
    /// <c>Increment</c> - the doc's stock-sync semantics are an authoritative overwrite of the
    /// reported quantities, not a delta.
    /// </summary>
    private static List<PatchOperation> BuildStockSyncPatch(ItemStockInventory aggregate)
    {
        List<PatchOperation> operations =
        [
            PatchOperation.Set("/B2CAVL", aggregate.B2CAvailable),
            PatchOperation.Set("/B2CPrepared", aggregate.B2CPrepared),
            PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
        ];

        if (aggregate.B2CAvailableToSell is not null)
        {
            operations.Add(PatchOperation.Set("/B2CAvailableToSell", aggregate.B2CAvailableToSell));
        }

        return operations;
    }

    /// <summary>
    /// Quantity fields read before <c>mutate</c> runs, so the patch builders below can emit an atomic
    /// <c>PatchOperation.Increment</c> delta (post minus pre) instead of the post-mutation absolute
    /// value - never last-write-wins for a quantity/allocation field (cosmos-db.instructions.md §9/§10).
    /// </summary>
    private readonly record struct QuantitySnapshot(
        int B2BAllocated, int B2BPrepared, int B2BUsedShare, int B2CAllocated, int B2CPrepared)
    {
        public static QuantitySnapshot Capture(ItemStockInventory aggregate) => new(
            aggregate.B2BAllocated, aggregate.B2BPrepared, aggregate.B2BUsedShare,
            aggregate.B2CAllocated, aggregate.B2CPrepared);
    }

    /// <summary>
    /// The canonical re-read-and-reapply retry loop (integration-resiliency.instructions.md §2):
    /// re-fetches the aggregate on every attempt so <paramref name="mutate"/> is applied against
    /// fresh state, and only retries on a genuine <see cref="ConcurrencyException"/> - this is the
    /// fix for the "PreCondition failed" issue this port addresses, since no prior mutation path
    /// existed to have this loop in the first place. <paramref name="buildPatchOperations"/> builds the
    /// minimal Increment-based Patch operation list for whatever <paramref name="mutate"/> just changed,
    /// from the pre-mutation <see cref="QuantitySnapshot"/> and the now-mutated aggregate - a full
    /// <c>ReplaceAsync</c> would overwrite fields other applications sharing this container concurrently
    /// wrote (cosmos-db.instructions.md §10).
    /// </summary>
    private async Task ApplyAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        Action<ItemStockInventory> mutate,
        Func<QuantitySnapshot, ItemStockInventory, IReadOnlyList<PatchOperation>> buildPatchOperations,
        CancellationToken cancellationToken)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                logger.LogWarning(
                    "No ItemStockInventory record found for {Id} - skipping mutation.", id);

                return;
            }

            var before = QuantitySnapshot.Capture(aggregate);

            try
            {
                mutate(aggregate);
            }
            catch (InsufficientItemStockException ex)
            {
                logger.LogWarning(ex, "Insufficient item stock for {Id} - skipping mutation.", id);

                return;
            }
            catch (ItemStockShareExhaustedException ex)
            {
                logger.LogWarning(ex, "B2B used-share exhausted for {Id} - skipping mutation.", id);

                return;
            }

            try
            {
                await repository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!, buildPatchOperations(before, aggregate), cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                logger.LogInformation("Applied mutation to ItemStockInventory {Id}.", id);

                return;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "Concurrency conflict applying mutation to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }
}
