using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.DependencyInjection;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="InfrastructureServiceCollectionExtensions.AddInfrastructure"/> -
/// the top-level composition-root entry point that wires resilience pipelines, Cosmos DB, Blob Storage,
/// the audit pipeline, dynamic validation, the Nexus dedup client, and all Kafka/Service Bus messaging
/// (see this file's own remarks). Its own method body is a straight-line sequence of calls into each
/// sub-system's own <c>Add*</c> extension (each covered by its own test file) plus one health check
/// registration - these tests exercise that sequence end to end with a complete configuration and spot-
/// check a representative registration from each sub-system, rather than re-testing every sub-system's
/// own internals here.
/// </summary>
public class InfrastructureServiceCollectionExtensionsTests
{
    private static IConfiguration BuildConfiguration() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Application:AppName"] = "IIS.WMS.Consumer",
                ["Application:AppId"] = "wms-consumer",
                ["CosmosDb:AccountEndpoint"] = "https://localhost:8081/",
                ["CosmosDb:DatabaseName"] = "InventoryDb",
                ["CosmosDb:EmulatorKey"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ["BlobStorage:Hot:AccountUri"] = "https://devstoreaccount1.blob.core.windows.net",
                ["BlobStorage:Cold:AccountUri"] = "https://devstoreaccount1.blob.core.windows.net",
                ["Nexus:Deduplication:BaseUrl"] = "https://nexus.example.com",
                ["Nexus:Deduplication:OAuthEndpoint"] = "https://nexus.example.com/oauth/token",
                ["Nexus:Deduplication:ClientId"] = "client-id",
                ["Nexus:Deduplication:ClientSecret"] = "client-secret",
                ["Nexus:Deduplication:Scope"] = "dedupe",
                ["Kafka:BootstrapServers"] = "localhost:9092",
                ["ServiceBus:ConnectionString"] =
                    "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123",
                ["ServiceBus:QueueName"] = "inventory-events",
                ["ServiceBus:BulkInventoryImport:QueueName"] = "inventory-bulk-import",
            })
            .Build();

    [Fact(DisplayName = "AddInfrastructure returns the same collection for chaining and registers without throwing given a complete configuration")]
    public void AddInfrastructure_CompleteConfiguration_RegistersWithoutThrowing()
    {
        var services = new ServiceCollection();

        var result = services.AddInfrastructure(BuildConfiguration());

        Assert.Same(services, result);
    }

    [Fact(DisplayName = "AddInfrastructure binds ApplicationOptions from the Application section")]
    public void AddInfrastructure_Registered_BindsApplicationOptions()
    {
        var services = new ServiceCollection();
        services.AddInfrastructure(BuildConfiguration());

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<ApplicationOptions>>().Value;

        Assert.Equal("IIS.WMS.Consumer", options.AppName);
        Assert.Equal("wms-consumer", options.AppId);
    }

    [Fact(DisplayName = "AddInfrastructure registers a cosmos-db health check tagged 'api'")]
    public void AddInfrastructure_Registered_CosmosHealthCheckTaggedApi()
    {
        var services = new ServiceCollection();
        services.AddInfrastructure(BuildConfiguration());

        var provider = services.BuildServiceProvider();
        var healthCheckOptions = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        var registration = Assert.Single(healthCheckOptions.Registrations, r => r.Name == "cosmos-db");
        Assert.Contains("api", registration.Tags);
    }

    [Theory(DisplayName = "AddInfrastructure wires up a representative top-level registration from each sub-system")]
    [InlineData(typeof(ICosmosContainerFactory))]
    [InlineData(typeof(IAuditTrailWriter))]
    [InlineData(typeof(IEventValidationTemplateService))]
    [InlineData(typeof(IDeduplicationService))]
    [InlineData(typeof(IServiceBusRelayPublisher))]
    public void AddInfrastructure_Registered_SubSystemServiceTypesPresent(Type serviceType)
    {
        var services = new ServiceCollection();
        services.AddInfrastructure(BuildConfiguration());

        Assert.Contains(services, d => d.ServiceType == serviceType);
    }

    [Fact(DisplayName = "AddInfrastructure registers the three named Polly resilience pipelines")]
    public void AddInfrastructure_Registered_ResiliencePipelinesResolve()
    {
        var services = new ServiceCollection();
        services.AddInfrastructure(BuildConfiguration());

        var provider = services.BuildServiceProvider();
        var pipelineProvider = provider.GetRequiredService<ResiliencePipelineProvider<string>>();

        Assert.NotNull(pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish));
        Assert.NotNull(pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload));
        Assert.NotNull(pipelineProvider.GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp));
    }
}
