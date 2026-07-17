namespace IIS.WMS.Consumer.Domain.Common;

/// <summary>The kind of Cosmos DB mutation an <see cref="Aggregates.AuditEntry"/> records.</summary>
public enum AuditOperation
{
    Create,
    Upsert,
    Replace,
    Patch,
    Delete,
}
