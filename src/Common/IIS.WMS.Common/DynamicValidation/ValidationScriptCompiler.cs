using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Common.DynamicValidation;

/// <inheritdoc cref="IValidationScriptCompiler"/>
public sealed class ValidationScriptCompiler : IValidationScriptCompiler
{
    private readonly ILogger<ValidationScriptCompiler> logger;
    private readonly ScriptOptions templateScriptOptions;

    /// <summary>
    /// Builds the references/imports every template compiles against once, at construction (the
    /// compiler is a singleton, so this still runs exactly once per process) - the compile-time
    /// surface of the script contract. The fixed baseline covers the globals' member types
    /// (<see cref="Messaging.HeaderLookup"/>, <see cref="ILogger"/>, this assembly for
    /// <see cref="ValidationScriptGlobals"/>/<see cref="Messaging.WellKnownHeaderNames"/>), the C#
    /// runtime binder that <c>dynamic</c> member access on <c>x</c> compiles down to, and - for the
    /// <c>services</c> global - the DI extension methods (<c>GetRequiredService&lt;T&gt;()</c>).
    /// <paramref name="referenceProviders"/> extends this per transport (e.g. Consumer's own
    /// <c>IDeduplicationService</c> assembly/import) - see <see cref="IValidationScriptReferenceProvider"/>.
    /// </summary>
    /// <param name="referenceProviders">Every transport-specific extension to the compile-time surface, registered by that transport's own DI setup.</param>
    /// <param name="logger">Logger for compile events.</param>
    public ValidationScriptCompiler(IEnumerable<IValidationScriptReferenceProvider> referenceProviders, ILogger<ValidationScriptCompiler> logger)
    {
        this.logger = logger;

        var options = ScriptOptions.Default
            .WithReferences(
                typeof(object).Assembly,
                typeof(System.Linq.Enumerable).Assembly,
                typeof(Microsoft.CSharp.RuntimeBinder.Binder).Assembly,
                typeof(ILogger).Assembly,
                typeof(Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions).Assembly,
                typeof(ValidationScriptGlobals).Assembly)
            .WithImports(
                "System",
                "System.Collections.Generic",
                "System.Linq",
                "System.Threading",
                "System.Threading.Tasks",
                "Microsoft.Extensions.DependencyInjection",
                "Microsoft.Extensions.Logging",
                "IIS.WMS.Common.Correlation",
                "IIS.WMS.Common.Messaging");

        foreach (var provider in referenceProviders)
        {
            options = options.AddReferences(provider.Assemblies).AddImports(provider.Imports);
        }

        templateScriptOptions = options;
    }

    /// <inheritdoc />
    public ScriptRunner<bool> Compile(string templateName, string code)
    {
        logger.LogDebug("Compiling validation template {TemplateName} ({CodeLength} chars).", templateName, code.Length);

        var script = CSharpScript.Create<bool>(code, templateScriptOptions, typeof(ValidationScriptGlobals));

        var errors = script.Compile()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .Select(diagnostic => diagnostic.ToString())
            .ToArray();

        if (errors.Length > 0)
        {
            throw new TemplateCompilationException(templateName, errors);
        }

        logger.LogInformation("Compiled validation template {TemplateName}.", templateName);

        return script.CreateDelegate();
    }
}
