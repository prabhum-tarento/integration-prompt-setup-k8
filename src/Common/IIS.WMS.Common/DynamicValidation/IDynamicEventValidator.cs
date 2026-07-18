using IIS.WMS.Common.Messaging;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Common.DynamicValidation;

/// <summary>
/// A transport's dynamic-validation step: looks up the <c>{transport}/{identifier}.cs</c>
/// template in the hot-tier validation-template container and, when one exists, executes its C# code
/// against the message. Runs right after the transport's own schema/message validation and follows
/// the same result contract (integration-resiliency.instructions.md): <b>throw</b> is a hard
/// validation failure (dead-letter + commit), <b>false</b> is "valid but deliberately not relayed",
/// and a missing template simply means no dynamic rule - the message passes.
/// </summary>
public interface IDynamicEventValidator
{
    /// <summary>Executes the transport/identifier's stored validation template against one message, if such a template exists.</summary>
    /// <param name="transport">The transport this message arrived on (<c>"Kafka"</c> or <c>"ServiceBus"</c>) - the folder segment of the template's blob path.</param>
    /// <param name="identifier">The transport-specific lookup key - the Kafka <c>Type</c> header value for Kafka, the queue name for Service Bus - the file segment of the template's blob path. Empty means no template can apply.</param>
    /// <param name="message">The deserialized event message - the template's <c>x</c> global.</param>
    /// <param name="headers">The consumed message's headers - the template's <c>header</c> global.</param>
    /// <param name="messageLogger">The consumer's logger - the template's <c>_log</c> global, so template log lines carry the consumer's category and the ambient correlation scope.</param>
    /// <param name="scopedServices">The message's DI scope - the template's <c>services</c> global, so a template resolves the same scoped instances (e.g. <c>ICorrelationContext</c>) the consumer itself uses for this message.</param>
    /// <param name="cancellationToken">Token to cancel the template lookup/execution.</param>
    /// <returns>The template's verdict, or <see langword="true"/> when no template exists for this transport/identifier (or dynamic validation is disabled).</returns>
    /// <exception cref="TemplateCompilationException">The stored template doesn't compile.</exception>
    /// <exception cref="Exception">Whatever the template itself throws to reject the message as a hard failure.</exception>
    Task<bool> ValidateAsync(
        string transport, string identifier, object message, HeaderLookup? headers, ILogger messageLogger, IServiceProvider scopedServices, CancellationToken cancellationToken);
}
