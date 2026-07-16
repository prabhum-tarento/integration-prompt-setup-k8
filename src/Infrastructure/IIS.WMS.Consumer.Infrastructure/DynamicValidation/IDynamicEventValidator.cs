using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// The Kafka consumer's dynamic-validation step: looks up the <c>{schemaName}/{eventType}.cs</c>
/// template in the hot-tier validation-template container and, when one exists, executes its C# code
/// against the message. Runs right after the schema handler's own <c>ValidateAsync</c> and follows
/// the exact same result contract (integration-resiliency.instructions.md §1): <b>throw</b> is a hard
/// validation failure (dead-letter + commit), <b>false</b> is "valid but deliberately not relayed",
/// and a missing template simply means no dynamic rule - the message passes.
/// </summary>
public interface IDynamicEventValidator
{
    /// <summary>Executes the schema/event type's stored validation template against one message, if such a template exists.</summary>
    /// <param name="schemaName">The resolved schema handler's <c>SchemaName</c> - the folder segment of the template's blob path.</param>
    /// <param name="eventType">The message's Kafka <c>Type</c> header value - the file segment of the template's blob path. Empty means no template can apply.</param>
    /// <param name="message">The deserialized event message - the template's <c>x</c> global.</param>
    /// <param name="headers">The consumed Kafka message's headers - the template's <c>header</c> global.</param>
    /// <param name="messageLogger">The consumer's logger - the template's <c>_log</c> global, so template log lines carry the consumer's category and the ambient correlation scope.</param>
    /// <param name="scopedServices">The message's DI scope - the template's <c>services</c> global, so a template resolves the same scoped instances (e.g. <c>ICorrelationContext</c>) the consumer itself uses for this message.</param>
    /// <param name="cancellationToken">Token to cancel the template lookup/execution.</param>
    /// <returns>The template's verdict, or <see langword="true"/> when no template exists for this schema/event type (or dynamic validation is disabled).</returns>
    /// <exception cref="Application.Exceptions.TemplateCompilationException">The stored template doesn't compile.</exception>
    /// <exception cref="Exception">Whatever the template itself throws to reject the message as a hard failure.</exception>
    Task<bool> ValidateAsync(
        string schemaName, string eventType, object message, Headers? headers, ILogger messageLogger, IServiceProvider scopedServices, CancellationToken cancellationToken);
}
