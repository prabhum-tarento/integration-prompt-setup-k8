using System.Reflection;
using FluentValidation;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.Application.DependencyInjection;

/// <summary>Composition-root entry point for the Application layer's own services - called from <c>Program.cs</c>, never invoked mid-request.</summary>
public static class ApplicationServiceCollectionExtensions
{
    /// <summary>Registers MediatR (for domain-event dispatch), FluentValidation validators, and the Application layer's own services.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        var assembly = Assembly.GetExecutingAssembly();

        services.AddMediatR(config => config.RegisterServicesFromAssembly(assembly));
        services.AddValidatorsFromAssembly(assembly);

        services.AddSingleton(TimeProvider.System);
        services.AddScoped<ICorrelationContext, CorrelationContext>();
        services.AddScoped<IDomainEventDispatcher, DomainEventDispatcher>();
        services.AddScoped<IInventoryEventService, InventoryEventService>();
        services.AddScoped<IBulkInventoryImportService, BulkInventoryImportService>();

        return services;
    }
}
