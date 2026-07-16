namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>Bound from the <c>DynamicValidation</c> configuration section - all defaults are usable without any configuration present.</summary>
public sealed class DynamicValidationOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "DynamicValidation";

    /// <summary>
    /// Master switch for the Kafka consumer's dynamic-validation step. When off, every message skips
    /// the template lookup entirely (as if no template existed) - the template CRUD API stays
    /// available either way, so templates can be staged before being turned on.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// How long one template lookup (found and compiled, found and broken, or confirmed missing) is
    /// reused before Blob Storage is consulted again. The trade-off: every message within the window
    /// validates against the cached result with zero storage calls, and a template created/updated/
    /// deleted through the API takes up to this long to affect the running consumer.
    /// </summary>
    public TimeSpan CacheDuration { get; init; } = TimeSpan.FromSeconds(60);
}
