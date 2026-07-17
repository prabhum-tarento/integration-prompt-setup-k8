using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="CosmosDbServiceCollectionExtensions.AddCosmosDb"/> - the default
/// (latency-sensitive) and keyed bulk-import (throughput-tuned) <see cref="CosmosClient"/>/
/// <see cref="ICosmosContainerFactory"/> pairs, and the scoped repository registrations
/// (cosmos-db.instructions.md §2/§12). Constructing a <see cref="CosmosClient"/> here never makes a
/// network call - the SDK connects lazily - so a syntactically valid emulator-style endpoint/key is
/// enough to exercise these factories without a real Cosmos account.
/// </summary>
public class CosmosDbServiceCollectionExtensionsTests
{
    private const string EmulatorKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private static IConfiguration BuildConfiguration(bool includeCosmosSection = true)
    {
        if (!includeCosmosSection)
        {
            return new ConfigurationBuilder().Build();
        }

        return new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["CosmosDb:AccountEndpoint"] = "https://localhost:8081/",
                ["CosmosDb:DatabaseName"] = "InventoryDb",
                ["CosmosDb:EmulatorKey"] = EmulatorKey,
            })
            .Build();
    }

    private static IServiceProvider BuildProvider(string environmentName, IConfiguration? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var env = Substitute.For<IHostEnvironment>();
        env.EnvironmentName.Returns(environmentName);
        services.AddSingleton(env);

        var result = services.AddCosmosDb(configuration ?? BuildConfiguration());

        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    [Fact(DisplayName = "AddCosmosDb builds the default CosmosClient under Development (Gateway mode, cert bypass)")]
    public void AddCosmosDb_Development_DefaultClientResolves()
    {
        var provider = BuildProvider(Environments.Development);

        var client = provider.GetRequiredService<CosmosClient>();

        Assert.NotNull(client);
    }

    [Fact(DisplayName = "AddCosmosDb builds the keyed bulk CosmosClient under Development (Gateway mode, cert bypass)")]
    public void AddCosmosDb_Development_BulkClientResolves()
    {
        var provider = BuildProvider(Environments.Development);

        var client = provider.GetRequiredKeyedService<CosmosClient>(CosmosDbServiceCollectionExtensions.BulkCosmosClientKey);

        Assert.NotNull(client);
    }

    [Fact(DisplayName = "AddCosmosDb builds the default CosmosClient outside Development (no Gateway-mode override)")]
    public void AddCosmosDb_Production_DefaultClientResolves()
    {
        var provider = BuildProvider(Environments.Production);

        var client = provider.GetRequiredService<CosmosClient>();

        Assert.NotNull(client);
    }

    [Fact(DisplayName = "AddCosmosDb builds the keyed bulk CosmosClient outside Development (no Gateway-mode override)")]
    public void AddCosmosDb_Production_BulkClientResolves()
    {
        var provider = BuildProvider(Environments.Production);

        var client = provider.GetRequiredKeyedService<CosmosClient>(CosmosDbServiceCollectionExtensions.BulkCosmosClientKey);

        Assert.NotNull(client);
    }

    [Fact(DisplayName = "Resolving the default CosmosClient throws when the CosmosDb section is missing")]
    public void AddCosmosDb_MissingSection_DefaultClientThrowsOnResolve()
    {
        var provider = BuildProvider(Environments.Development, BuildConfiguration(includeCosmosSection: false));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<CosmosClient>());
        Assert.Contains("CosmosDb", exception.Message);
    }

    [Fact(DisplayName = "Resolving the keyed bulk CosmosClient throws when the CosmosDb section is missing")]
    public void AddCosmosDb_MissingSection_BulkClientThrowsOnResolve()
    {
        var provider = BuildProvider(Environments.Development, BuildConfiguration(includeCosmosSection: false));

        var exception = Assert.Throws<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<CosmosClient>(CosmosDbServiceCollectionExtensions.BulkCosmosClientKey));
        Assert.Contains("CosmosDb", exception.Message);
    }

    [Fact(DisplayName = "AddCosmosDb registers the default ICosmosContainerFactory as a singleton, distinct from the keyed bulk factory")]
    public void AddCosmosDb_Registered_ContainerFactoriesAreDistinctSingletons()
    {
        var provider = BuildProvider(Environments.Development);

        var defaultFactory1 = provider.GetRequiredService<ICosmosContainerFactory>();
        var defaultFactory2 = provider.GetRequiredService<ICosmosContainerFactory>();
        var bulkFactory = provider.GetRequiredKeyedService<ICosmosContainerFactory>(CosmosDbServiceCollectionExtensions.BulkCosmosClientKey);

        Assert.Same(defaultFactory1, defaultFactory2);
        Assert.NotSame(defaultFactory1, bulkFactory);
    }

    [Theory(DisplayName = "AddCosmosDb registers each repository interface as Scoped, backed by its concrete type")]
    [InlineData(typeof(IInventoryEventRepository), typeof(InventoryEventRepository))]
    [InlineData(typeof(IOrderArchiveRepository), typeof(OrderArchiveRepository))]
    [InlineData(typeof(IBulkInventoryImportRepository), typeof(InventoryBulkImportItemRepository))]
    public void AddCosmosDb_Registered_RepositoriesAreScoped(Type serviceType, Type implementationType)
    {
        var services = new ServiceCollection();
        services.AddCosmosDb(BuildConfiguration());

        var descriptor = Assert.Single(services, d => d.ServiceType == serviceType);

        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
        Assert.Equal(implementationType, descriptor.ImplementationType);
    }

    [Fact(DisplayName = "BulkCosmosClientKey is the well-known 'bulk' keyed-service key")]
    public void BulkCosmosClientKey_Value_IsBulk()
    {
        Assert.Equal("bulk", CosmosDbServiceCollectionExtensions.BulkCosmosClientKey);
    }
}
