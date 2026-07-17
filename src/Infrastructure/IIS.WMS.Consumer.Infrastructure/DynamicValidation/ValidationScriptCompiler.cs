using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <inheritdoc cref="IValidationScriptCompiler"/>
public sealed class ValidationScriptCompiler(ILogger<ValidationScriptCompiler> logger) : IValidationScriptCompiler
{
    /// <summary>
    /// References/imports every template compiles against - the compile-time surface of the script
    /// contract, built once since it never varies per template. References cover the globals'
    /// member types (<see cref="Confluent.Kafka.Headers"/>, <see cref="ILogger"/>, this assembly for
    /// <see cref="ValidationScriptGlobals"/>/<see cref="KafkaHeaderNames"/>), the C# runtime
    /// binder that <c>dynamic</c> member access on <c>x</c> compiles down to, and - for the
    /// <c>services</c> global - the DI extension methods (<c>GetRequiredService&lt;T&gt;()</c>) plus
    /// the Application and Common assemblies so their service interfaces are nameable as type
    /// arguments. Imports match: a template writes <c>KafkaHeaderNames.Type</c>, <c>_log.LogInformation(...)</c>, or
    /// <c>services.GetRequiredService&lt;ICorrelationContext&gt;()</c> bare, without its own
    /// <c>using</c> lines.
    /// </summary>
    private static readonly ScriptOptions TemplateScriptOptions = ScriptOptions.Default
        .WithReferences(
            typeof(object).Assembly,
            typeof(System.Linq.Enumerable).Assembly,
            typeof(Microsoft.CSharp.RuntimeBinder.Binder).Assembly,
            typeof(ILogger).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions).Assembly,
            typeof(Application.Common.IDeduplicationService).Assembly,
            typeof(IIS.WMS.Common.Correlation.ICorrelationContext).Assembly,
            typeof(Confluent.Kafka.Headers).Assembly,
            typeof(ValidationScriptGlobals).Assembly)
        .WithImports(
            "System",
            "System.Collections.Generic",
            "System.Linq",
            "System.Threading",
            "System.Threading.Tasks",
            "Microsoft.Extensions.DependencyInjection",
            "Microsoft.Extensions.Logging",
            "Confluent.Kafka",
            "IIS.WMS.Common.Correlation",
            "IIS.WMS.Consumer.Application.Common",
            "IIS.WMS.Consumer.Infrastructure.Messaging.Kafka");

    /// <inheritdoc />
    public ScriptRunner<bool> Compile(string templateName, string code)
    {
        logger.LogDebug("Compiling validation template {TemplateName} ({CodeLength} chars).", templateName, code.Length);

        var script = CSharpScript.Create<bool>(code, TemplateScriptOptions, typeof(ValidationScriptGlobals));

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
