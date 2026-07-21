namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Cosmos DB persistence shape for a fulfilment-level segmentation rule (cosmos-db.instructions.md §3).</summary>
public class FulfilmentLevelSegmentationDocument : ICosmosDocument
{
    /// <summary>Deterministic item id.</summary>
    public string Id { get; set; } = default!;

    /// <summary>Cosmos partition key value - <c>SEG_FU_{FulfilmentCode}_{HallmarkCode}</c>.</summary>
    public string Category { get; set; } = default!;

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

    /// <summary>Cosmos's system-managed optimistic-concurrency token.</summary>
    public string? ETag { get; set; }
}
