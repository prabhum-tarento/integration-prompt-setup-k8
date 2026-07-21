namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>Fulfilment-level segmentation rule: store/e-commerce share and leverage thresholds for one fulfilment/hallmark/item/country-of-origin combination.</summary>
public class FulfilmentLevelSegmentation
{
    /// <summary>Fulfilment code, part of this rule's partition key.</summary>
    public string FulfilmentCode { get; set; } = default!;

    /// <summary>Hallmark code, part of this rule's partition key.</summary>
    public string HallmarkCode { get; set; } = default!;

    /// <summary>Item code this rule applies to.</summary>
    public string ItemCode { get; set; } = default!;

    /// <summary>Country-of-origin code this rule applies to.</summary>
    public string CountryOfOriginCode { get; set; } = default!;

    /// <summary>Percentage of stock share allocated to store fulfilment.</summary>
    public decimal StoreSharePercentage { get; set; }

    /// <summary>Percentage of stock share allocated to e-commerce fulfilment.</summary>
    public decimal EcomSharePercentage { get; set; }

    /// <summary>Threshold percentage that triggers a segmentation recalculation, if configured.</summary>
    public decimal? ThresholdPercentage { get; set; }

    /// <summary>Percentage of store share that may be borrowed against when extending an oversold e-commerce reservation.</summary>
    public decimal? StoreLeveragePercentage { get; set; }

    /// <summary>Whether this rule is currently active.</summary>
    public bool IsActive { get; set; }
}

/// <summary>
/// Selective-column projection of <see cref="FulfilmentLevelSegmentation"/> - only the two fields the
/// store-leverage lookup needs (cosmos-db.instructions.md §8), not the full rule.
/// </summary>
public class FulfilmentLevelSegmentationStoreLeveragePercentage
{
    /// <summary>Percentage of store share that may be borrowed against when extending an oversold e-commerce reservation.</summary>
    public decimal? StoreLeveragePercentage { get; set; }

    /// <summary>Whether the matching rule is currently active.</summary>
    public bool IsActive { get; set; }
}
