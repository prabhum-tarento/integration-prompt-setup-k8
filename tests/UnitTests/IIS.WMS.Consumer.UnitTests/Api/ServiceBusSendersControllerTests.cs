using IIS.WMS.Consumer.Api.Controllers;
using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Application.Messaging.Dtos;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>Request/response-mapping tests for <see cref="ServiceBusSendersController"/>, with <see cref="IServiceBusSenderCacheService"/> mocked (aspnet-rest-apis.instructions.md "Testing this layer").</summary>
public class ServiceBusSendersControllerTests
{
    private readonly IServiceBusSenderCacheService cacheService = Substitute.For<IServiceBusSenderCacheService>();
    private readonly ServiceBusSendersController sut;

    public ServiceBusSendersControllerTests()
    {
        sut = new ServiceBusSendersController(cacheService, Substitute.For<ILogger<ServiceBusSendersController>>());
    }

    [Fact(DisplayName = "Get returns 200 with every cached-sender entry")]
    public void Get_SendersCached_ReturnsOkWithEntries()
    {
        var entries = new List<ServiceBusSenderCacheEntry>
        {
            new("KafkaConsumerHostedService", ["inventory-events"]),
        };
        cacheService.ListCachedSenders().Returns(entries);

        var result = sut.Get();

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(entries, okResult.Value);
    }

    [Fact(DisplayName = "ClearAsync clears the cache and returns 200 with the pre-clear entries")]
    public async Task ClearAsync_SendersCached_ClearsAndReturnsOkWithPreClearEntries()
    {
        var entries = new List<ServiceBusSenderCacheEntry>
        {
            new("KafkaConsumerHostedService", ["inventory-events"]),
        };
        cacheService.ListCachedSenders().Returns(entries);

        var result = await sut.ClearAsync(CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(entries, okResult.Value);
        await cacheService.Received(1).ClearCachedSendersAsync(Arg.Any<CancellationToken>());
    }
}
