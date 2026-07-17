using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Correctness tests for <see cref="CosmosContainerFactory"/> - the singleton container cache every repository resolves through.</summary>
public class CosmosContainerFactoryTests
{
    private const string DatabaseName = "InventoryDb";

    private static IOptions<CosmosDbOptions> BuildOptions() =>
        Options.Create(new CosmosDbOptions { DatabaseName = DatabaseName });

    [Fact(DisplayName = "GetContainer resolves the container from the client on first use")]
    public void GetContainer_FirstCall_ResolvesFromClient()
    {
        var client = Substitute.For<CosmosClient>();
        var container = Substitute.For<Container>();
        client.GetContainer(DatabaseName, "InventoryEvents").Returns(container);
        var factory = new CosmosContainerFactory(client, BuildOptions());

        var result = factory.GetContainer("InventoryEvents");

        Assert.Same(container, result);
        client.Received(1).GetContainer(DatabaseName, "InventoryEvents");
    }

    [Fact(DisplayName = "GetContainer caches the resolved container - a second call for the same name does not re-resolve from the client")]
    public void GetContainer_CalledTwiceForSameName_ResolvesFromClientOnlyOnce()
    {
        var client = Substitute.For<CosmosClient>();
        var container = Substitute.For<Container>();
        client.GetContainer(DatabaseName, "InventoryEvents").Returns(container);
        var factory = new CosmosContainerFactory(client, BuildOptions());

        var first = factory.GetContainer("InventoryEvents");
        var second = factory.GetContainer("InventoryEvents");

        Assert.Same(container, first);
        Assert.Same(container, second);
        client.Received(1).GetContainer(DatabaseName, "InventoryEvents");
    }

    [Fact(DisplayName = "GetContainer resolves a distinct container for each distinct container name")]
    public void GetContainer_DifferentNames_ResolvesSeparateContainers()
    {
        var client = Substitute.For<CosmosClient>();
        var containerA = Substitute.For<Container>();
        var containerB = Substitute.For<Container>();
        client.GetContainer(DatabaseName, "A").Returns(containerA);
        client.GetContainer(DatabaseName, "B").Returns(containerB);
        var factory = new CosmosContainerFactory(client, BuildOptions());

        var resultA = factory.GetContainer("A");
        var resultB = factory.GetContainer("B");

        Assert.Same(containerA, resultA);
        Assert.Same(containerB, resultB);
        Assert.NotSame(resultA, resultB);
    }
}
