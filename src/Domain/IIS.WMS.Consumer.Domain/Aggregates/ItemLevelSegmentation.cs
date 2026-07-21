namespace IIS.WMS.Consumer.Domain.Aggregates;

public class ItemLevelSegmentation
{
    public string FulfilmentCode { get; set; } = default!;
    public string HallmarkCode { get; set; } = default!;
    public string ItemCode { get; set; } = default!;
    public string CountryOfOriginCode { get; set; } = default!;
    public int? EcomShare { get; set; }
    public decimal? StoreLeveragePercentage { get; set; }
    public decimal? ThresholdPercentage { get; set; }
    public bool IsOMNI { get; set; }

    #region Updated through inventory
    public int? CurrentOmniStock { get; set; }
    public int? CurrentEcomStock { get; set; }
    public int? StoreShare { get; set; }
    public int? EcomStatus { get; set; }
    public bool IsExtended { get; set; }
    public string Notes { get; set; } = default!;

    public int? InTransit { get; set; }

    public DateTime? LastModified { get; set; }

    public bool IsActive { get; set; }
    #endregion
    public string? ETag { get; set; }
}