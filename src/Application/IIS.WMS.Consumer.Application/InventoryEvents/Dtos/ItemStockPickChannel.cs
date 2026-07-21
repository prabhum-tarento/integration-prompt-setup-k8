namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>Which allocated/prepared pair a pick moves quantity between - mirrors Reflex's <c>PICKEDB2B</c>/<c>PICKEDB2C</c> change types.</summary>
public enum ItemStockPickChannel
{
    B2B,
    B2C,
}
