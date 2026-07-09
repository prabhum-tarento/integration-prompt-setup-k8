using Asp.Versioning;
using IIS.WMS.Consumer.Api.ExceptionHandling;
using IIS.WMS.Consumer.Api.Filters;
using IIS.WMS.Consumer.Api.Middleware;
using IIS.WMS.Consumer.Application.DependencyInjection;
using IIS.WMS.Consumer.Infrastructure.DependencyInjection;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Scalar.AspNetCore;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Structured JSON logging with the correlation id enriched by CorrelationIdMiddleware
// (engineering-standards.instructions.md §6).
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .WriteTo.Console(new Serilog.Formatting.Json.JsonFormatter()));

builder.Services.AddHttpContextAccessor();

builder.Services
    .AddControllers(options => options.Filters.Add<ValidationFilter>());

// [ApiController]'s automatic model-state validation must be disabled for ValidationFilter to be
// what actually runs (aspnet-rest-apis.instructions.md "Validation").
builder.Services.Configure<ApiBehaviorOptions>(options => options.SuppressModelStateInvalidFilter = true);

builder.Services.AddApiVersioning(options =>
{
    options.DefaultApiVersion = new ApiVersion(1.0);
    options.AssumeDefaultVersionWhenUnspecified = true;
    options.ReportApiVersions = true;
}).AddMvc().AddApiExplorer(options =>
{
    options.GroupNameFormat = "'v'VVV";
    options.SubstituteApiVersionInUrl = true;
});

builder.Services.AddOpenApi();

// All three of AddProblemDetails, AddExceptionHandler, and the app.UseExceptionHandler() call
// below are required together - the DI registrations alone do nothing without the middleware
// (aspnet-rest-apis.instructions.md "Global exception handling").
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<GlobalExceptionHandler>();

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = builder.Configuration["Authentication:Authority"];
        options.Audience = builder.Configuration["Authentication:Audience"];
    });

// Authenticated by default (engineering-standards.instructions.md §6) - health endpoints opt out
// explicitly with .AllowAnonymous() below, per aspnet-rest-apis.instructions.md "Health checks".
builder.Services.AddAuthorizationBuilder()
    .SetFallbackPolicy(new AuthorizationPolicyBuilder()
        .RequireAuthenticatedUser()
        .Build());

builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("IIS.WMS.Consumer.Api"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddOtlpExporter())
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddOtlpExporter());

var app = builder.Build();

// CorrelationIdMiddleware runs first so the id exists before any exception can be thrown, and
// before UseExceptionHandler() so the handler can read it from ICorrelationContext
// (aspnet-rest-apis.instructions.md "Correlation ID").
app.UseMiddleware<CorrelationIdMiddleware>();
app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}
else
{
    app.UseHttpsRedirection();
}

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();

// Every health endpoint is reachable without authentication - the AKS kubelet calling these
// probes has no bearer token (aspnet-rest-apis.instructions.md "Health checks").
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false })
    .AllowAnonymous();

// Tagged per Pod per kubernetes-deployment-best-practices.instructions.md's 3-Deployment target
// topology (see InfrastructureServiceCollectionExtensions) - this skeleton hosts all three
// workloads in one process, so all three are mapped here for now.
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("api"),
}).AllowAnonymous();

app.MapHealthChecks("/health/ready/kafka-consumer", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("kafka-consumer"),
}).AllowAnonymous();

app.MapHealthChecks("/health/ready/service-bus-consumer", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("service-bus-consumer"),
}).AllowAnonymous();

app.Run();
