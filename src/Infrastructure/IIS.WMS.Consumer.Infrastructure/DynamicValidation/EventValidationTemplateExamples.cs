using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// The worked examples the examples endpoint serves - kept here, next to
/// <see cref="ValidationScriptGlobals"/> and <see cref="ValidationScriptCompiler"/>, because they ARE
/// documentation of that contract: every example must compile through the real
/// <see cref="ValidationScriptCompiler.Compile"/> (a unit test enforces this), so a
/// contract change that breaks an example fails the build's test run instead of silently serving
/// stale documentation. Descriptions spell out the two things template authors trip over: the
/// three outcomes (true = relay, false = valid-but-skip, throw = hard failure/dead-letter), and
/// that <c>x</c> is <c>dynamic</c> - so extension methods (LINQ, <c>_log.LogInformation</c>) need
/// dynamic values cast to a real type before the call binds.
/// </summary>
public static class EventValidationTemplateExamples
{
    /// <summary>The example catalog, in reading order from simplest to fullest.</summary>
    public static IReadOnlyList<EventValidationTemplateExample> All { get; } =
    [
        new(
            "Field check - return a verdict",
            "The smallest useful template. 'x' is the deserialized event message, injected per message " +
            "by the consumer; dereference its fields directly (x is dynamic, so no casts or usings are " +
            "needed to read a field). Return true to relay the message to Service Bus, false to skip it.",
            """
            // x is the deserialized event message - read its fields directly.
            return !string.IsNullOrEmpty(x.Reference);
            """),

        new(
            "Hard failure - throw",
            "Throwing is a hard validation failure: the consumer logs it at Critical, writes the " +
            "message's JSON to the hot-tier dead-letter container for the out-of-band watcher to " +
            "reprocess, and commits the offset forward. Throw for malformed/invalid data; return false " +
            "instead for data that is valid but shouldn't be relayed.",
            """
            if (x.Id == null)
            {
                throw new ApplicationException("Invalid Request - Id is required.");
            }

            return true;
            """),

        new(
            "Deliberate skip - return false",
            "Returning false means 'valid, but deliberately not relayed' - logged at Information, " +
            "offset committed, no dead-letter write. Use it to filter traffic, e.g. by producer. " +
            "'header' is the message's Kafka headers object, injected per message; read individual " +
            "values with the TryGetHeader helper and the KafkaHeaderNames constants, both available " +
            "without any using directives.",
            """
            // Skip (don't relay) everything from a producer we intentionally ignore.
            if (TryGetHeader(header, KafkaHeaderNames.AppId) == "legacy-producer")
            {
                return false;
            }

            return true;
            """),

        new(
            "Validate against Kafka headers",
            "Any header can participate in the verdict. TryGetHeader returns null for an absent " +
            "header, so null-check before trusting a value. KafkaHeaderNames exposes the well-known " +
            "names: Type, CorrelationId, DeduplicationId, AppId, EventKey.",
            """
            var eventType = TryGetHeader(header, KafkaHeaderNames.Type);
            var correlationId = TryGetHeader(header, KafkaHeaderNames.CorrelationId);

            // Require both a recognized event type and a correlation id.
            return eventType == "inventory.InventoryStateChanged" && !string.IsNullOrEmpty(correlationId);
            """),

        new(
            "Log through the injected logger - _log",
            "'_log' is the consumer's ILogger, injected per message - lines it writes carry the " +
            "message's correlation id automatically via the ambient log context. Use structured " +
            "message templates, not string interpolation. Because x is dynamic and the Log* methods " +
            "are extension methods, cast a field (e.g. (string)x.Reference) when passing it as a log " +
            "argument - extension methods don't bind on dynamic values.",
            """
            if (string.IsNullOrEmpty(x.WarehouseId))
            {
                _log.LogWarning("Skipping {Reference} - missing WarehouseId.", (string)x.Reference);
                return false;
            }

            _log.LogInformation("Validated {Reference} from {AppId}.", (string)x.Reference, TryGetHeader(header, KafkaHeaderNames.AppId));
            return true;
            """),

        new(
            "Check a collection field with LINQ",
            "Same dynamic-binding rule as logging: cast a collection field to IEnumerable<dynamic> " +
            "before calling LINQ operators on it, then cast each element's fields to their real types " +
            "inside the lambda.",
            """
            // Extension methods (LINQ) don't bind on dynamic - cast the collection first.
            var lines = (IEnumerable<dynamic>)x.ItemLines;

            return lines.All(line => (int)line.Quantity >= 0);
            """),

        new(
            "Resolve services from DI - services",
            "'services' is this message's DI scope (IServiceProvider), injected per message. Resolve " +
            "anything registered in the container with services.GetRequiredService<T>() - the DI " +
            "extension methods and the IIS.WMS.Consumer.Application.Common/IIS.WMS.Common.Correlation " +
            "interfaces are already imported. Scoped services resolve to this message's own instances " +
            "(ICorrelationContext below carries this message's correlation id), and templates can " +
            "await async service calls - the whole template runs as async code.",
            """
            // Scoped service: this message's own correlation context.
            var correlation = services.GetRequiredService<ICorrelationContext>();

            // Singleton service, awaited - e.g. an extra dedup check keyed on a body field,
            // which the header-based check that already ran can't see.
            var dedup = services.GetRequiredService<IDeduplicationService>();
            var alreadySeen = await dedup.IsDuplicateAsync(
                "IIS_WMS_TemplateCheck", (string)x.Id, correlation.CorrelationId, CancellationToken.None);

            if (alreadySeen)
            {
                _log.LogInformation("Skipping {Id} - already processed under {CorrelationId}.", (string)x.Id, correlation.CorrelationId);
                return false;
            }

            return true;
            """),

        new(
            "Full example - all injected objects and all three outcomes",
            "Combines everything: field checks on x, a hard-failure throw, a header read, and a log " +
            "line - the shape most real templates end up with.",
            """
            if (string.IsNullOrEmpty(x.Reference))
            {
                return true;
            }
            else if (x.Id == null)
            {
                throw new ApplicationException("Invalid Request");
            }
            else
            {
                _log.LogInformation("Valid request from {EventType}.", TryGetHeader(header, KafkaHeaderNames.Type));
                return true;
            }
            """),
    ];
}
