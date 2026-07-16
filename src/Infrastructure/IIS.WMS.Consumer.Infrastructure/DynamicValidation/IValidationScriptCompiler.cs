using Microsoft.CodeAnalysis.Scripting;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// Compiles an event validation template's C# code into an executable
/// <see cref="ScriptRunner{T}"/> against the <see cref="ValidationScriptGlobals"/> contract. One seam
/// shared by both consumers of template code: the CRUD service (compile-check before storing, so a
/// broken script is rejected at the API instead of dead-lettering live traffic) and the Kafka
/// consumer's <see cref="IDynamicEventValidator"/> (compile-and-cache before executing).
/// </summary>
public interface IValidationScriptCompiler
{
    /// <summary>Compiles <paramref name="code"/> into a runnable validation script.</summary>
    /// <param name="templateName">The template the code belongs to (e.g. <c>InventoryStateChangedEvent/inventory.InventoryStateChanged</c>) - used in the compilation-failure diagnostics.</param>
    /// <param name="code">The template's C# script - returns <see cref="bool"/>, against the <see cref="ValidationScriptGlobals"/> globals.</param>
    /// <returns>The compiled, reusable runner - thread-safe, so one compilation can serve every consumer worker.</returns>
    /// <exception cref="Application.Exceptions.TemplateCompilationException">The code has compiler errors - carries one entry per error diagnostic.</exception>
    ScriptRunner<bool> Compile(string templateName, string code);
}
