namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>Shared location shape for the §3.6/3.7/3.8 publish payloads (docs/InventoryStateChangedFullQueueTrigger.md) - a plain copy of the wire event's location, not a reference to the Infrastructure-layer type.</summary>
public sealed record PublishLocation(string Id, string Type);

/// <summary>Shared state/status shape for the §3.6/3.7/3.8 publish payloads - a plain copy of the wire event's state snapshot, not a reference to the Infrastructure-layer type.</summary>
public sealed record PublishStateSnapshot(string State, string Status);
