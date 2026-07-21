using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

public interface IItemLevelSegmentationRepository
{
    Task<ItemLevelSegmentation?> GetItemLevelFulfilmentyByCategory(string fulfilment, string hallMarkType, string itemCode, string coo);
}