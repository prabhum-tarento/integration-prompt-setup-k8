using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Common.DynamicValidation;

/// <summary>
/// Registration for the blob-stored event validation templates' compile-and-cache runtime: the
/// script compiler and the dynamic validator every transport (Kafka consumer, Service Bus consumer,
/// the planned Producer) runs after its own message validation. Each transport's own DI setup calls
/// this and additionally registers its own <see cref="IValidationScriptReferenceProvider"/> for
/// whatever transport-specific services its stored templates need to resolve via the
/// <c>services</c> global.
/// </summary>
public static class DynamicValidationServiceCollectionExtensions
{
    /// <summary>Registers the validation-template compiler and a transport's dynamic validator.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>DynamicValidation</c> section (optional - every option has a usable default).</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddDynamicEventValidation(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<DynamicValidationOptions>(configuration.GetSection(DynamicValidationOptions.SectionName));

        services.AddSingleton<IValidationScriptCompiler, ValidationScriptCompiler>();
        services.AddSingleton<IDynamicEventValidator, DynamicEventValidator>();

        return services;
    }
}
