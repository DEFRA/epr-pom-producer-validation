using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using EPR.ProducerContentValidation.Application;
using EPR.ProducerContentValidation.Application.Clients;
using EPR.ProducerContentValidation.Application.Config;
using EPR.ProducerContentValidation.Application.DTOs.SplitFunction;
using EPR.ProducerContentValidation.Application.Handlers;
using EPR.ProducerContentValidation.FunctionApp;
using EPR.ProducerContentValidation.FunctionApp.Utils;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Http.Resilience;
using Microsoft.Extensions.Options;
using Microsoft.FeatureManagement;
using Polly;
using Polly.Timeout;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, config) =>
    config.ReadFrom.Configuration(context.Configuration));

builder.WebHost.UseUrls("http://+:8085");

// Trust material must be loaded before anything creates outbound TLS connections (e.g. MongoDB).
builder.Services.LoadCustomTrustStoreFromEnvironment();

builder.Services.AddFeatureManagement();
builder.Services.AddApplicationServices();
builder.Services.AddFunctionServices();

builder.Services.AddHttpClient<ICompanyDetailsApiClient, CompanyDetailsApiClient>((sp, c) =>
{
    var companyDetailsApiConfig = sp.GetRequiredService<IOptions<CompanyDetailsApiConfig>>().Value;
    c.BaseAddress = new Uri(companyDetailsApiConfig.BaseUrl);
    c.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
})
.AddHttpMessageHandler<CompanyDetailsApiAuthorisationHandler>()
.AddResilienceHandler("CompanyDetailsResiliencePipeline", BuildResiliencePipeline<CompanyDetailsApiConfig>(o => TimeSpan.FromSeconds(o.Timeout)));

builder.Services.AddHealthChecks();
builder.Services.AddSingleton<ValidateProducerContentFunction>();
builder.Services.AddHostedService<ProducerValidationWorker>();

var app = builder.Build();

app.UseSerilogRequestLogging();

app.MapHealthChecks("/health", new HealthCheckOptions());

app.MapPost("/validate-producer-content", async (
    ProducerValidationInRequest request,
    ValidateProducerContentFunction function,
    bool skipApiCall = false) =>
{
    var result = await function.PerformValidationAsync(request, skipApiCall);
    return Results.Ok(result);
});

await app.RunAsync();

[ExcludeFromCodeCoverage]
static Action<ResiliencePipelineBuilder<HttpResponseMessage>, ResilienceHandlerContext> BuildResiliencePipeline<TConfig>(Func<TConfig, TimeSpan> timeoutSelector)
    where TConfig : class =>
    (builder, context) =>
    {
        var sp = context.ServiceProvider;
        var timeout = timeoutSelector(sp.GetRequiredService<IOptions<TConfig>>()?.Value);
        BuildResiliencePipelineCore(builder, timeout);
    };

[ExcludeFromCodeCoverage]
static void BuildResiliencePipelineCore(ResiliencePipelineBuilder<HttpResponseMessage> builder, TimeSpan? timeout = null)
{
    builder.AddRetry(new HttpRetryStrategyOptions
    {
        MaxRetryAttempts = 4,
        Delay = TimeSpan.FromSeconds(2),
        BackoffType = DelayBackoffType.Exponential,
        ShouldHandle = args =>
        {
            bool shouldHandle;
            var exception = args.Outcome.Exception;
            if (exception is TimeoutRejectedException ||
               (exception is OperationCanceledException && exception.Source == "System.Private.CoreLib" && exception.InnerException is TimeoutException))
            {
                shouldHandle = true;
            }
            else
            {
                shouldHandle = HttpClientResiliencePredicates.IsTransient(args.Outcome);
            }

            return new ValueTask<bool>(shouldHandle);
        },
    });

    if (timeout is not null)
    {
        builder.AddTimeout(timeout.Value);
    }
}
