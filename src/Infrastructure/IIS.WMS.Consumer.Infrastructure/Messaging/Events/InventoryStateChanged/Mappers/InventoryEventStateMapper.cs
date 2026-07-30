using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

/// <summary>
/// Converts this consumer's own wire-contract state/status enums
/// (<see cref="InventoryEventStockState"/>/<see cref="InventoryEventStockStatus"/>) to the
/// Domain-layer equivalents (<see cref="DomainEnums.State"/>/<see cref="DomainEnums.Status"/>) the
/// §3.5/§3.6 Application-layer ports accept - those ports must not depend on Infrastructure wire
/// types (dotnet-architecture-good-practices.instructions.md).
/// </summary>
internal static class InventoryEventStateMapper
{
    public static DomainEnums.State ToDomainState(InventoryEventStockState state) => state switch
    {
        InventoryEventStockState.Unknown => DomainEnums.State.UNKNOWN,
        InventoryEventStockState.Available => DomainEnums.State.AVAILABLE,
        InventoryEventStockState.Blocked => DomainEnums.State.BLOCKED,
        InventoryEventStockState.Inspection => DomainEnums.State.INSPECTION,
        InventoryEventStockState.Scrap => DomainEnums.State.SCRAP,
        InventoryEventStockState.Rework => DomainEnums.State.REWORK,
        InventoryEventStockState.Remelt => DomainEnums.State.REMELT,
        InventoryEventStockState.Stone => DomainEnums.State.STONE,
        InventoryEventStockState.AvailableToSell => DomainEnums.State.AVAILABLETOSELL,
        _ => DomainEnums.State.UNKNOWN,
    };

    public static DomainEnums.Status ToDomainStatus(InventoryEventStockStatus status) => status switch
    {
        InventoryEventStockStatus.Unknown => DomainEnums.Status.UNKNOWN,
        InventoryEventStockStatus.Pickable => DomainEnums.Status.PICKABLE,
        InventoryEventStockStatus.Held => DomainEnums.Status.HELD,
        InventoryEventStockStatus.Prepared => DomainEnums.Status.PREPARED,
        InventoryEventStockStatus.Hallmarking => DomainEnums.Status.HALLMARKING,
        InventoryEventStockStatus.Allocated => DomainEnums.Status.ALLOCATED,
        InventoryEventStockStatus.Invoiced => DomainEnums.Status.INVOICED,
        _ => DomainEnums.Status.UNKNOWN,
    };
}
