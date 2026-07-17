using FluentValidation;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.DependencyInjection;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using MediatR;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Registration tests for <see cref="ApplicationServiceCollectionExtensions.AddApplication"/>, asserting
/// against a real <see cref="ServiceCollection"/> rather than a mocked <see cref="IServiceCollection"/>.
/// </summary>
public class ApplicationServiceCollectionExtensionsTests
{
    [Fact(DisplayName = "AddApplication returns the same service collection instance for chaining")]
    public void AddApplication_Always_ReturnsSameInstanceForChaining()
    {
        var services = new ServiceCollection();

        var result = services.AddApplication();

        Assert.Same(services, result);
    }

    [Fact(DisplayName = "AddApplication registers TimeProvider.System as a singleton")]
    public void AddApplication_Always_RegistersTimeProviderSingleton()
    {
        var services = new ServiceCollection();

        services.AddApplication();

        var descriptor = Assert.Single(services, sd => sd.ServiceType == typeof(TimeProvider));
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Same(TimeProvider.System, descriptor.ImplementationInstance);
    }

    [Theory(DisplayName = "AddApplication registers each Application service with Scoped lifetime")]
    [InlineData(typeof(ICorrelationContext), typeof(CorrelationContext))]
    [InlineData(typeof(IDomainEventDispatcher), typeof(DomainEventDispatcher))]
    [InlineData(typeof(IInventoryEventService), typeof(InventoryEventService))]
    [InlineData(typeof(IBulkInventoryImportService), typeof(BulkInventoryImportService))]
    public void AddApplication_Always_RegistersServiceWithScopedLifetime(Type serviceType, Type implementationType)
    {
        var services = new ServiceCollection();

        services.AddApplication();

        var descriptor = Assert.Single(services, sd => sd.ServiceType == serviceType);
        Assert.Equal(implementationType, descriptor.ImplementationType);
        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
    }

    [Fact(DisplayName = "AddApplication registers MediatR's IMediator for domain-event dispatch")]
    public void AddApplication_Always_RegistersMediator()
    {
        var services = new ServiceCollection();

        services.AddApplication();

        Assert.Contains(services, sd => sd.ServiceType == typeof(IMediator));
    }

    [Fact(DisplayName = "AddApplication registers FluentValidation validators discovered from the Application assembly")]
    public void AddApplication_Always_RegistersFluentValidationValidators()
    {
        var services = new ServiceCollection();

        services.AddApplication();

        Assert.Contains(services, sd => sd.ServiceType == typeof(IValidator<CreateInventoryEventRequest>));
        Assert.Contains(services, sd => sd.ServiceType == typeof(IValidator<ReserveStockRequest>));
    }
}
