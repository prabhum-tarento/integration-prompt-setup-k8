using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// Registration for the blob-stored event validation templates: the CRUD service behind the
/// event-validation-templates API, and (via <see cref="Common.DynamicValidationServiceCollectionExtensions"/>)
/// the compile-and-cache validator the Kafka and Service Bus consumers each run after their own
/// message validation. Everything here is a singleton - the compiler and CRUD service are stateless
/// (their dependencies are already singletons, same reasoning as <c>BlobFileStore</c>'s
/// registration), and the validator's template cache must be process-wide to be a cache at all.
/// </summary>
public static class DynamicValidationServiceCollectionExtensions
{
    /// <summary>Registers the validation-template compiler, this consumer's dynamic validator, its script reference provider, and the template CRUD service.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>DynamicValidation</c> section (optional - every option has a usable default).</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddDynamicValidation(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddDynamicEventValidation(configuration);
        services.AddSingleton<IValidationScriptReferenceProvider, ConsumerValidationScriptReferenceProvider>();
        services.AddSingleton<IEventValidationTemplateService, EventValidationTemplateService>();

        return services;
    }
}
