using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

namespace IIS.WMS.Consumer.Infrastructure;

/// <summary>
/// Bound from the <c>Application</c> configuration section - this service's own identity, as opposed
/// to <see cref="ICorrelationContext.AppId"/>, which is the <i>producing</i> application's id read off
/// the Kafka <c>App-Id</c> header (integration-resiliency.instructions.md §4). <see cref="AppId"/>
/// is the fallback <see cref="ConsumerHostedService"/> uses when that header is missing or empty, so a
/// relayed event never carries a blank <c>AppId</c> downstream.
/// </summary>
public sealed class ApplicationOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Application";

    /// <summary>Human-readable name for this service - enriched onto every Serilog log line (Program.cs) so log lines from this service are identifiable when aggregated alongside other services.</summary>
    public string AppName { get; init; } = default!;

    /// <summary>This service's own id - used as <see cref="ICorrelationContext.AppId"/>'s fallback when the Kafka <c>App-Id</c> header is missing or empty.</summary>
    public string AppId { get; init; } = default!;
}
