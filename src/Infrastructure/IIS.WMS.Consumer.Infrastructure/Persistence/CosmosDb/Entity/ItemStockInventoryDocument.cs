namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

public class ItemStockInventoryDocument : ICosmosDocument
{
    public string Id { get; set; } = default!;

    public string Category { get; set; } = default!;
    public string ItemCode { get; set; } = default!;
    public string FulfilmentId { get; set; } = default!;
    public string COO { get; set; } = default!;
    public string Hallmark { get; set; } = default!;
    public int? B2BAVL { get; set; }
    public int? B2CAVL { get; set; }
    public int? B2COrg { get; set; }
    public int? B2CExtended { get; set; }
    public int? B2CAllocated { get; set; }
    public int? B2BAllocated { get; set; }
    public int? B2CPrepared { get; set; }
    public int? B2BPrepared { get; set; }
    public int? InternalHallmarkAllocated { get; set; }
    public int? InTransit { get; set; }
    public int? B2CThreshold { get; set; }
    public bool IsExtended { get; set; }
    public int? B2BUsedShare { get; set; }
    public int? Inspection { get; set; }

    public int? PSC { get; set; }
    public string Timestamp { get; set; } = default!;
    public bool? IsPOSM { get; set; }

    public string? ETag { get; set; }
}
