namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Every Cosmos DB container name this service reads/writes, centralized here (cosmos-db.instructions.md
/// §5) rather than declared per-repository - each container is provisioned externally (Bicep/Terraform),
/// not created by this app, so its name is a fixed constant, not configuration.
/// </summary>
public static class CosmosContainerNames
{
    /// <summary>Base name for the per-fulfilment-code ItemStockInventory containers - see docs/InventoryStateChanged-OrderTracking-Relay.md for why this is split by <see cref="ItemStockInventorySuffix"/> rather than one shared container.</summary>
    public const string ItemStockInventory = "ItemStockInventory";

    public const string AuditLog = "AuditLog";

    public const string InventoryEvents = "InventoryEvents";

    public const string OrderArchive = "OrderArchive";

    public const string MessageArchive = "MessageArchive";

    public const string Rules = "Rules";

    public const string BulkInventoryImports = "BulkInventoryImports";

    /// <summary>Allow-listed fulfilment codes that split the ItemStockInventory container - each member name is the exact suffix appended to <see cref="ItemStockInventory"/> (e.g. <see cref="Edc"/> - "ItemStockInventoryEDC").</summary>
    public enum ItemStockInventorySuffix
    {
        Edc,
        Tdc,
        Adc,
        CaEcom,
        Brz3Pl,
    }

    /// <summary>Resolves the per-fulfilment-code container name for ItemStockInventory (e.g. "ItemStockInventoryEDC"), or throws if <paramref name="fulfilmentCode"/> isn't one of the allow-listed <see cref="ItemStockInventorySuffix"/> values.</summary>
    public static string GetItemStockInventoryContainerName(string fulfilmentCode)
    {
        if (!Enum.TryParse<ItemStockInventorySuffix>(fulfilmentCode, ignoreCase: true, out var suffix))
        {
            throw new ArgumentException(
                $"Unrecognized fulfilment code '{fulfilmentCode}' for ItemStockInventory container resolution. " +
                $"Allowed codes: {string.Join(", ", Enum.GetNames<ItemStockInventorySuffix>())}.",
                nameof(fulfilmentCode));
        }

        return ItemStockInventory + suffix.ToString().ToUpperInvariant();
    }
}
