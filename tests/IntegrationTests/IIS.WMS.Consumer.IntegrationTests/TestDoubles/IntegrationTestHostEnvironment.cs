using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles;

/// <summary>
/// Minimal <see cref="IHostEnvironment"/> for the pipeline test's stand-alone <c>ServiceCollection</c>
/// (not a full <c>WebApplicationFactory</c> host) - only needed when a dependency runs in
/// <c>BackendMode.ConnectionString</c>, since <c>AddCosmosDb</c>/<c>AddBlobStorage</c>'s real client
/// factories read <see cref="IHostEnvironment.IsDevelopment"/> to decide between the local emulator key
/// and <c>DefaultAzureCredential</c> - always reports <c>Development</c>, matching what an integration
/// test pointed at a local emulator/Azurite needs.
/// </summary>
public sealed class IntegrationTestHostEnvironment : IHostEnvironment
{
    public string EnvironmentName { get; set; } = Environments.Development;
    public string ApplicationName { get; set; } = "IIS.WMS.Consumer.IntegrationTests";
    public string ContentRootPath { get; set; } = AppContext.BaseDirectory;
    public IFileProvider ContentRootFileProvider { get; set; } = new PhysicalFileProvider(AppContext.BaseDirectory);
}
