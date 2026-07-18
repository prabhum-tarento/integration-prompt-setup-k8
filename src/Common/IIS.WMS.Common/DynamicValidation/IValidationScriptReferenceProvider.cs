using System.Reflection;

namespace IIS.WMS.Common.DynamicValidation;

/// <summary>
/// Extends the assembly references/imports a stored validation template compiles against, beyond the
/// fixed baseline <see cref="ValidationScriptCompiler"/> always includes (the BCL, the C# runtime
/// binder, <c>ILogger</c>/DI extensions, and this assembly itself for <see cref="ValidationScriptGlobals"/>/
/// <c>WellKnownHeaderNames</c>). Common has no <c>ProjectReference</c> to any transport-specific
/// project, so it can't hardcode a reference to e.g. Consumer's own <c>IDeduplicationService</c> - each
/// transport's own DI registration (Consumer's <c>AddDynamicValidation</c>, and eventually a Producer
/// project's own) registers one implementation supplying whatever service interfaces its own templates
/// need to resolve via the <c>services</c> global.
/// </summary>
public interface IValidationScriptReferenceProvider
{
    /// <summary>Assemblies to add to every template's compile-time references.</summary>
    IReadOnlyList<Assembly> Assemblies { get; }

    /// <summary>Namespaces to add to every template's implicit <c>using</c>s.</summary>
    IReadOnlyList<string> Imports { get; }
}
