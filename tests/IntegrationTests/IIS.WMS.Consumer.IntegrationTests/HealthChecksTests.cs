using System.Net;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>Verifies every health endpoint is reachable without authentication, per the AllowAnonymous requirement in aspnet-rest-apis.instructions.md "Health checks".</summary>
public class HealthChecksTests(CustomWebApplicationFactory factory) : IClassFixture<CustomWebApplicationFactory>
{
    private readonly HttpClient client = factory.CreateClient();

    [Fact(DisplayName = "GET /health/live returns 200 without authentication")]
    public async Task GetHealthLive_NoAuthHeader_ReturnsOk()
    {
        var response = await client.GetAsync("/health/live");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact(DisplayName = "GET /health/ready returns 200 without authentication")]
    public async Task GetHealthReady_NoAuthHeader_ReturnsOk()
    {
        var response = await client.GetAsync("/health/ready");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact(DisplayName = "GET /health/ready/kafka-consumer returns 200 without authentication")]
    public async Task GetHealthReadyKafkaConsumer_NoAuthHeader_ReturnsOk()
    {
        var response = await client.GetAsync("/health/ready/kafka-consumer");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
