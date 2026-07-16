using Confluent.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// The globals every event validation template script executes against - each public member here is
/// directly usable as an identifier inside a template's C# code. The member names deliberately break
/// this repo's naming conventions (<c>x</c>, <c>header</c>, <c>_log</c>): they ARE the script
/// contract every stored <c>{SchemaName}/{EventType}.cs</c> template is written against, so renaming
/// one is a breaking change to every template in the container, not a refactor.
/// </summary>
/// <param name="message">The deserialized event message - exposed to scripts as <c>x</c>.</param>
/// <param name="headers">The consumed Kafka message's headers - exposed to scripts as <c>header</c>.</param>
/// <param name="logger">The consumer's logger - exposed to scripts as <c>_log</c>; the per-message correlation id is already on the ambient <c>LogContext</c>.</param>
/// <param name="serviceProvider">This message's DI scope - exposed to scripts as <c>services</c>, so a template can resolve scoped services (e.g. <c>ICorrelationContext</c>) alongside singletons.</param>
/// <example>
/// <code>
/// if (string.IsNullOrEmpty(x.Reference)) { return true; }
/// else if (x.Id == null) { throw new ApplicationException("Invalid Request"); }
/// else { _log.LogInformation($"Valid request from {TryGetHeader(header, KafkaHeaderNames.Type)}"); return true; }
/// </code>
/// </example>
public sealed class ValidationScriptGlobals(object message, Headers? headers, ILogger logger, IServiceProvider serviceProvider)
{
#pragma warning disable IDE1006 // Naming Styles - x/header/_log are the published script contract, see the class remarks.
    /// <summary>The deserialized event message. <c>dynamic</c> so a template can dereference schema fields (<c>x.Reference</c>) without compile-time knowledge of the concrete event type.</summary>
    public dynamic x { get; } = message;

    /// <summary>The consumed Kafka message's headers, or <see langword="null"/> if none were sent - read individual values via <see cref="TryGetHeader"/>.</summary>
    public Headers? header { get; } = headers;

    /// <summary>Structured logger a template can emit its own log lines through.</summary>
    public ILogger _log { get; } = logger;

    /// <summary>
    /// This message's DI scope. A template resolves anything registered in the container via
    /// <c>services.GetRequiredService&lt;T&gt;()</c> (the extension methods and the
    /// <c>IIS.WMS.Consumer.Application.Common</c> service interfaces are already imported) and can
    /// <c>await</c> what it resolves - the whole template runs as async code. Scoped services resolve
    /// to this message's own instances, the same ones the consumer itself uses.
    /// </summary>
    public IServiceProvider services { get; } = serviceProvider;
#pragma warning restore IDE1006

    /// <summary>Reads a Kafka header's value as a UTF-8 string, if present - callable from a template as a bare <c>TryGetHeader(header, KafkaHeaderNames.Type)</c>.</summary>
    /// <param name="headers">Headers on the consumed Kafka message, or <see langword="null"/> if none were sent.</param>
    /// <param name="key">Header name - see <see cref="KafkaHeaderNames"/>.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    public static string? TryGetHeader(Headers? headers, string key)
    {
        return headers is not null && headers.TryGetLastBytes(key, out var bytes)
            ? System.Text.Encoding.UTF8.GetString(bytes)
            : null;
    }
}
