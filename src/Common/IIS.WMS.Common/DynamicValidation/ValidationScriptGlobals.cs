using IIS.WMS.Common.Messaging;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Common.DynamicValidation;

/// <summary>
/// The script globals every stored validation template compiles against - <c>x</c>, <c>header</c>,
/// <c>_log</c>, and <c>services</c> are the template's entire injected surface.
/// </summary>
/// <remarks>
/// Renaming any member here is a breaking change to every template in the container, not a refactor -
/// every stored template's source references these exact names.
/// </remarks>
/// <param name="message">The deserialized event message - the template's <c>x</c> global.</param>
/// <param name="headers">The message's headers, transport-adapted - the template's <c>header</c> global.</param>
/// <param name="logger">The consumer's logger - the template's <c>_log</c> global.</param>
/// <param name="serviceProvider">The message's DI scope - the template's <c>services</c> global.</param>
public sealed class ValidationScriptGlobals(object message, HeaderLookup? headers, ILogger logger, IServiceProvider serviceProvider)
{
    public dynamic x { get; } = message;
    public HeaderLookup? header { get; } = headers;
    public ILogger _log { get; } = logger;
    public IServiceProvider services { get; } = serviceProvider;

    /// <summary>Reads one header's value, if present - the template's <c>TryGetHeader</c> helper.</summary>
    /// <param name="headers">The template's own <c>header</c> global.</param>
    /// <param name="key">Header name - see <see cref="WellKnownHeaderNames"/>.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    public static string? TryGetHeader(HeaderLookup? headers, string key) => headers?.TryGet(key);
}
