namespace IIS.WMS.Consumer.Application.Common;

/// <inheritdoc cref="ICorrelationContext"/>
public sealed class CorrelationContext : ICorrelationContext
{
    /// <inheritdoc />
    public string CorrelationId { get; private set; } = string.Empty;

    /// <inheritdoc />
    public void Set(string correlationId)
    {
        CorrelationId = correlationId;
    }
}
