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
            buildPatchOperations: aggregate => channel == ItemStockPickChannel.B2B
                ? BuildB2BPickPatch(aggregate)
                : BuildB2CPickPatch(aggregate),
            cancellationToken);

    /// <inheritdoc />
    public Task ApplyUnpickAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default) =>
        ApplyAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            aggregate => aggregate.Unpick(quantity, timeProvider.GetUtcNow().UtcDateTime),
            buildPatchOperations: aggregate =>
            [
                PatchOperation.Set("/B2BPrepared", aggregate.B2BPrepared),
                PatchOperation.Set("/B2BAllocated", aggregate.B2BAllocated),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ],
            cancellationToken);

    /// <summary>
    /// The exact field set <see cref="ItemStockInventory.PickB2B"/> mutates: allocated/prepared/modified
    /// always, plus used-share only when extension is active - see docs/InventoryStateChangedFullQueueTrigger.md.
    /// </summary>
    private static List<PatchOperation> BuildB2BPickPatch(ItemStockInventory aggregate)
    {
        List<PatchOperation> operations =
        [
            PatchOperation.Set("/B2BAllocated", aggregate.B2BAllocated),
            PatchOperation.Set("/B2BPrepared", aggregate.B2BPrepared),
            PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
        ];

        if (aggregate.IsExtended)
        {
            operations.Add(PatchOperation.Set("/B2BUsedShare", aggregate.B2BUsedShare));
        }

        return operations;
    }

    /// <summary>
    /// The exact field set <see cref="ItemStockInventory.PickB2C"/> mutates: prepared always, allocated
    /// always, modified always, and used-share only on the extended-oversell branch - see
    /// docs/InventoryStateChangedFullQueueTrigger.md.
    /// </summary>
    private static IReadOnlyList<PatchOperation> BuildB2CPickPatch(ItemStockInventory aggregate) =>
    [
        PatchOperation.Set("/B2CPrepared", aggregate.B2CPrepared),
        PatchOperation.Set("/B2CAllocated", aggregate.B2CAllocated),
        PatchOperation.Set("/B2BUsedShare", aggregate.B2BUsedShare),
        PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
    ];

    /// <summary>
    /// The canonical re-read-and-reapply retry loop (integration-resiliency.instructions.md §2):
    /// re-fetches the aggregate on every attempt so <paramref name="mutate"/> is applied against
    /// fresh state, and only retries on a genuine <see cref="ConcurrencyException"/> - this is the
    /// fix for the "PreCondition failed" issue this port addresses, since no prior mutation path
    /// existed to have this loop in the first place. <paramref name="buildPatchOperations"/> builds the
    /// minimal Patch operation list for whatever <paramref name="mutate"/> just changed - a full
    /// <c>ReplaceAsync</c> would overwrite fields other applications sharing this container concurrently
    /// wrote (cosmos-db.instructions.md §10).
    /// </summary>
    private async Task ApplyAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        Action<ItemStockInventory> mutate,
        Func<ItemStockInventory, IReadOnlyList<PatchOperation>> buildPatchOperations,
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
                    aggregate.Id, aggregate.Category, aggregate.ETag!, buildPatchOperations(aggregate), cancellationToken);
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
