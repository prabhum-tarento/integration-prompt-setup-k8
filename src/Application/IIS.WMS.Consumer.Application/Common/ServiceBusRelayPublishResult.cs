namespace IIS.WMS.Consumer.Application.Common;

/// <summary>Outcome of one <see cref="IServiceBusRelayPublisher"/> publish - lets the caller still log/measure the claim-check and publish steps it no longer performs itself.</summary>
/// <param name="WasOffloaded">Whether this message's payload exceeded the claim-check threshold and was uploaded to blob storage instead of traveling inline.</param>
/// <param name="BlobPath">The blob path the payload was uploaded to, or empty if it traveled inline.</param>
/// <param name="BlobOffloadDuration">How long the blob upload took, or <see cref="TimeSpan.Zero"/> if the payload traveled inline.</param>
/// <param name="PublishDuration">How long the Service Bus send itself took (for a batched publish, the whole batch's send).</param>
public sealed record ServiceBusRelayPublishResult(
    bool WasOffloaded, string BlobPath, TimeSpan BlobOffloadDuration, TimeSpan PublishDuration);
