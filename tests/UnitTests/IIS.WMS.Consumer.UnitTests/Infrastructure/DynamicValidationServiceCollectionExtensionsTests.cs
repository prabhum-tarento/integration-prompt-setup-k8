using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="DynamicValidationServiceCollectionExtensions.AddDynamicValidation"/> -
/// the blob-stored validation-template compiler, Kafka consumer dynamic validator, and template CRUD
/// service, all registered as singletons.
/// </summary>
public class DynamicValidationServiceCollectionExtensionsTests
{
    private static IServiceProvider BuildProvider(IConfiguration? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());

        var configurationRoot = configuration ?? new ConfigurationBuilder().Build();
        var result = services.AddDynamicValidation(configurationRoot);

        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    [Fact(DisplayName = "AddDynamicValidation registers the compiler, dynamic validator, and template service as singletons")]
    public void AddDynamicValidation_Registered_ResolvesAllThreeAsSingletons()
    {
        var provider = BuildProvider();

        var compiler1 = provider.GetRequiredService<IValidationScriptCompiler>();
        var compiler2 = provider.GetRequiredService<IValidationScriptCompiler>();
        Assert.IsType<ValidationScriptCompiler>(compiler1);
        Assert.Same(compiler1, compiler2);

        var validator1 = provider.GetRequiredService<IDynamicEventValidator>();
        var validator2 = provider.GetRequiredService<IDynamicEventValidator>();
        Assert.IsType<DynamicEventValidator>(validator1);
        Assert.Same(validator1, validator2);

        var templateService1 = provider.GetRequiredService<IEventValidationTemplateService>();
        var templateService2 = provider.GetRequiredService<IEventValidationTemplateService>();
        Assert.IsType<EventValidationTemplateService>(templateService1);
        Assert.Same(templateService1, templateService2);
    }

    [Fact(DisplayName = "AddDynamicValidation binds DynamicValidationOptions from the DynamicValidation section")]
    public void AddDynamicValidation_ConfiguredSection_BindsOptions()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["DynamicValidation:Enabled"] = "false",
                ["DynamicValidation:CacheDuration"] = "00:05:00",
            })
            .Build();

        var provider = BuildProvider(configuration);
        var options = provider.GetRequiredService<IOptions<DynamicValidationOptions>>().Value;

        Assert.False(options.Enabled);
        Assert.Equal(TimeSpan.FromMinutes(5), options.CacheDuration);
    }

    [Fact(DisplayName = "AddDynamicValidation leaves defaults in place when the DynamicValidation section is absent")]
    public void AddDynamicValidation_NoSection_UsesDefaults()
    {
        var provider = BuildProvider();
        var options = provider.GetRequiredService<IOptions<DynamicValidationOptions>>().Value;

        Assert.True(options.Enabled);
        Assert.Equal(TimeSpan.FromSeconds(60), options.CacheDuration);
    }
}
