namespace IIS.WMS.Common.Resilience;

/// <summary>Named keys for the Polly v8 pipelines registered against <c>ResiliencePipelineProvider&lt;string&gt;</c> - resolve by key, don't hardcode the string at each call site.</summary>
public static class ResiliencePipelines
{
    /// <summary>Pipeline for publishing to Azure Service Bus - retry plus circuit breaker on transient <c>ServiceBusException</c>s.</summary>
    public const string ServiceBusPublish = "service-bus-publish";

    /// <summary>Pipeline for Blob Storage uploads/downloads - retry on transient <c>RequestFailedException</c> status codes.</summary>
    public const string BlobUpload = "blob-upload";

    /// <summary>Pipeline for outbound HTTP calls to other services - retry on transient failures and result status codes.</summary>
    public const string OutboundHttp = "outbound-http";
}
