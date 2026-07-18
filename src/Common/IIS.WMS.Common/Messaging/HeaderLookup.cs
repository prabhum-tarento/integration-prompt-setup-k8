namespace IIS.WMS.Common.Messaging;

/// <summary>
/// Transport-neutral view over one message's headers - the dynamic-validation script contract's
/// <c>header</c> global (<c>IIS.WMS.Common.DynamicValidation.ValidationScriptGlobals</c>) reads
/// through this instead of a transport SDK type, so a stored template compiles and runs the same way
/// whether the message arrived over Kafka (<c>Confluent.Kafka.Headers</c>) or Service Bus
/// (<c>ApplicationProperties</c>). Each transport's consumer adapts its own native header type into
/// this at the call site; this type itself has no dependency on either SDK.
/// </summary>
/// <param name="values">The message's headers/application properties, already resolved to string values.</param>
public sealed class HeaderLookup(IReadOnlyDictionary<string, string> values)
{
    /// <summary>Shared empty instance for a message with no headers to look up.</summary>
    public static readonly HeaderLookup Empty = new(new Dictionary<string, string>());

    /// <summary>Reads one header's value, if present.</summary>
    /// <param name="key">Header name - see <see cref="WellKnownHeaderNames"/>.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    public string? TryGet(string key) => values.TryGetValue(key, out var value) ? value : null;
}
