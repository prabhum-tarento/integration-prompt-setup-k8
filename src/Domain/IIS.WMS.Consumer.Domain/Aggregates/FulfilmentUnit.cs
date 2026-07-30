namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>Fulfilment unit master record - resolves a fulfilment location's country of origin for the §3.7 OMS delta market field (docs/events/inventory.InventoryStateChanged.md).</summary>
public class FulfilmentUnit
{
    public string FulfilmentId { get; set; } = default!;
    public string CountryCode { get; set; } = default!;
}
