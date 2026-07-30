namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>Fulfilment-level segmentation rule: store/e-commerce share and leverage thresholds for one fulfilment/hallmark/item/country-of-origin combination.</summary>
public class CountryMaster
{
    public string Code { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string RegionCode { get; set; } = default!;
    public bool IsActive { get; set; }
    public bool IsAX12Market { get; set; }
}
