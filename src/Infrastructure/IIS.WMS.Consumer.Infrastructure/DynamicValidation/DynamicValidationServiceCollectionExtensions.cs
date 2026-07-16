using IIS.WMS.Consumer.Application.EventValidationTemplates;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// Registration for the blob-stored event validation templates: the CRUD service behind the
/// event-validation-templates API and the compile-and-cache validator the Kafka consumer runs after
/// each schema handler's own <c>ValidateAsync</c>. Everything here is a singleton - the compiler and
/// CRUD service are stateless (their dependencies are already singletons, same reasoning as
/// <c>BlobFileStore</c>'s registration), and the validator's template cache must be process-wide to
/// be a cache at all.
/// </summary>
public static class DynamicValidationServiceCollectionExtensions
{
    /// <summary>Registers the validation-template compiler, the Kafka consumer's dynamic validator, and the template CRUD service.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>DynamicValidation</c> section (optional - every option has a usable default).</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddDynamicValidation(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<DynamicValidationOptions>(configuration.GetSection(DynamicValidationOptions.SectionName));

        services.AddSingleton<IValidationScriptCompiler, ValidationScriptCompiler>();
        services.AddSingleton<IDynamicEventValidator, DynamicEventValidator>();
        services.AddSingleton<IEventValidationTemplateService, EventValidationTemplateService>();

        return services;
    }
}
