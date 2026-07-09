using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Storage.Blobs;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Hosts the real Api pipeline - routing, API versioning, <c>ValidationFilter</c>,
/// <c>GlobalExceptionHandler</c>, <c>CorrelationIdMiddleware</c> - per
/// aspnet-rest-apis.instructions.md "Testing this layer". Swaps out everything that would
/// otherwise need a live Cosmos DB, Service Bus namespace, Kafka broker, or Entra ID tenant, so
/// these tests exercise the middleware pipeline and controller wiring on their own merits. The
/// Kafka → Service Bus → Cosmos DB path itself (including the concurrency-conflict and
/// redelivery cases cosmos-db.instructions.md §13 and integration-resiliency.instructions.md §9
/// require) is covered separately by Testcontainers-based tests, which this class does not
/// replace.
/// </summary>
public sealed class CustomWebApplicationFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");

        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IHostedService>();
            services.RemoveAll<CosmosClient>();
            services.RemoveAll<ICosmosContainerFactory>();
            services.RemoveAll<ServiceBusClient>();
            services.RemoveAll<ServiceBusAdministrationClient>();
            services.RemoveAll<BlobServiceClient>();

            services.RemoveAll<IInventoryEventRepository>();
            services.AddSingleton<IInventoryEventRepository, InMemoryInventoryEventRepository>();

            services.RemoveAll<IFileStore>();

            services.Configure<HealthCheckServiceOptions>(options =>
            {
                options.Registrations.Clear();
                options.Registrations.Add(new HealthCheckRegistration("cosmos-db", new AlwaysHealthyCheck(), null, ["api"]));
                options.Registrations.Add(new HealthCheckRegistration("kafka-consumer", new AlwaysHealthyCheck(), null, ["kafka-consumer"]));
                options.Registrations.Add(new HealthCheckRegistration("service-bus", new AlwaysHealthyCheck(), null, ["service-bus-consumer"]));
            });

            // Swap the real JWT Bearer scheme for one that authenticates every request, so tests
            // can exercise the authenticated-by-default fallback policy without a real Entra ID token.
            services.AddAuthentication(TestAuthenticationHandler.SchemeName)
                .AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>(TestAuthenticationHandler.SchemeName, _ => { });
            services.PostConfigure<Microsoft.AspNetCore.Authentication.AuthenticationOptions>(options =>
            {
                options.DefaultAuthenticateScheme = TestAuthenticationHandler.SchemeName;
                options.DefaultChallengeScheme = TestAuthenticationHandler.SchemeName;
            });
        });
    }
}
