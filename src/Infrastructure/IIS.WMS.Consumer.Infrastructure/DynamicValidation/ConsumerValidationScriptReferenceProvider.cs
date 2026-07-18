using System.Reflection;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// Extends every stored validation template's compile-time surface with this consumer's own
/// <see cref="IDeduplicationService"/> - so a template can resolve it via
/// <c>services.GetRequiredService&lt;IDeduplicationService&gt;()</c>. Common can't reference this
/// itself (it has no <c>ProjectReference</c> to any transport-specific project), so this consumer
/// supplies it as its own <see cref="IValidationScriptReferenceProvider"/>.
/// </summary>
public sealed class ConsumerValidationScriptReferenceProvider : IValidationScriptReferenceProvider
{
    /// <inheritdoc />
    public IReadOnlyList<Assembly> Assemblies { get; } = [typeof(IDeduplicationService).Assembly];

    /// <inheritdoc />
    public IReadOnlyList<string> Imports { get; } = ["IIS.WMS.Consumer.Application.Common"];
}
