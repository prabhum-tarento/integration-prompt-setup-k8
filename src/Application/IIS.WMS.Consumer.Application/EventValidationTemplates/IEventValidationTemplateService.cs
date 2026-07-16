using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

namespace IIS.WMS.Consumer.Application.EventValidationTemplates;

/// <summary>
/// CRUD over the event validation templates stored as <c>{SchemaName}/{EventType}.cs</c> blobs in the
/// hot-tier validation-template container. Each template holds a C# script the Kafka consumer executes
/// against every message of that schema/event type right after the schema handler's own
/// <c>ValidateAsync</c> - so create/update compile the code first and reject anything that doesn't
/// build, rather than letting a broken script dead-letter live traffic. Implemented in Infrastructure
/// (blob-backed), same as <c>IFileStore</c>.
/// </summary>
public interface IEventValidationTemplateService
{
    /// <summary>Reads one template's code.</summary>
    /// <param name="schemaName">Schema the template applies to.</param>
    /// <param name="eventType">Kafka <c>Type</c> header value the template applies to.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    /// <returns>The template, or <see langword="null"/> if none is stored under that identity.</returns>
    Task<EventValidationTemplateResponse?> GetAsync(
        string schemaName, string eventType, CancellationToken cancellationToken = default);

    /// <summary>Lists every stored template's identity, optionally narrowed to one schema.</summary>
    /// <param name="schemaName">Schema to filter on, or <see langword="null"/> for every template.</param>
    /// <param name="cancellationToken">Token to cancel the listing.</param>
    /// <returns>The matching templates - empty if none are stored.</returns>
    Task<IReadOnlyList<EventValidationTemplateSummary>> ListAsync(
        string? schemaName = null, CancellationToken cancellationToken = default);

    /// <summary>Compiles and stores a new template.</summary>
    /// <param name="request">The template's identity and C# code.</param>
    /// <param name="cancellationToken">Token to cancel the create.</param>
    /// <returns>The stored template.</returns>
    /// <exception cref="Exceptions.TemplateCompilationException">The code doesn't compile - nothing is stored.</exception>
    /// <exception cref="Exceptions.ConflictException">A template already exists under that identity - use <see cref="UpdateAsync"/> to replace it.</exception>
    Task<EventValidationTemplateResponse> CreateAsync(
        CreateEventValidationTemplateRequest request, CancellationToken cancellationToken = default);

    /// <summary>Compiles and stores a replacement for an existing template's code.</summary>
    /// <param name="schemaName">Schema the template applies to.</param>
    /// <param name="eventType">Kafka <c>Type</c> header value the template applies to.</param>
    /// <param name="request">The replacement C# code.</param>
    /// <param name="cancellationToken">Token to cancel the update.</param>
    /// <returns>The stored template.</returns>
    /// <exception cref="Exceptions.TemplateCompilationException">The code doesn't compile - the stored template is left unchanged.</exception>
    /// <exception cref="Exceptions.NotFoundException">No template exists under that identity - use <see cref="CreateAsync"/> instead.</exception>
    Task<EventValidationTemplateResponse> UpdateAsync(
        string schemaName, string eventType, UpdateEventValidationTemplateRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Worked, compile-verified examples of the script contract - one per authoring pattern
    /// (pass/fail verdicts, hard-failure throws, deliberate skips, Kafka header reads, logging,
    /// collection checks), each showing how to use the objects injected into every script run
    /// (<c>x</c>, <c>header</c>, <c>_log</c>, plus the <c>TryGetHeader</c>/<c>KafkaHeaderNames</c>
    /// helpers). Static documentation, not stored templates - hence no <c>CancellationToken</c>.
    /// </summary>
    /// <returns>The examples, in reading order from simplest to fullest.</returns>
    IReadOnlyList<EventValidationTemplateExample> GetExamples();

    /// <summary>Deletes one template.</summary>
    /// <param name="schemaName">Schema the template applies to.</param>
    /// <param name="eventType">Kafka <c>Type</c> header value the template applies to.</param>
    /// <param name="cancellationToken">Token to cancel the delete.</param>
    /// <returns><see langword="true"/> if the template existed and was deleted; <see langword="false"/> if there was nothing to delete.</returns>
    Task<bool> DeleteAsync(
        string schemaName, string eventType, CancellationToken cancellationToken = default);
}
