using System.Net;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using EPR.ProducerContentValidation.Application.DTOs.SplitFunction;
using EPR.ProducerContentValidation.Application.Options;
using Microsoft.Extensions.Options;

namespace EPR.ProducerContentValidation.FunctionApp;

public class ProducerValidationWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly ServiceBusProcessor _processor;
    private readonly ValidateProducerContentFunction _function;
    private readonly ILogger<ProducerValidationWorker> _logger;

    public ProducerValidationWorker(
        IOptions<ServiceBusOptions> serviceBusOptions,
        ValidateProducerContentFunction function,
        ILogger<ProducerValidationWorker> logger)
    {
        // CDP only allows outbound traffic via the Squid HTTP proxy, which cannot tunnel
        // raw AMQP over TCP (port 5671). AMQP over WebSockets runs on port 443 and can be
        // tunnelled through the proxy via HTTP CONNECT, so the client must be told to use it.
        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };

        var proxyAddress = Environment.GetEnvironmentVariable("CDP_HTTPS_PROXY")
            ?? Environment.GetEnvironmentVariable("HTTP_PROXY");

        if (!string.IsNullOrWhiteSpace(proxyAddress))
        {
            clientOptions.WebProxy = BuildWebProxy(proxyAddress);
        }

        var client = new ServiceBusClient(serviceBusOptions.Value.ConnectionString, clientOptions);
        _processor = client.CreateProcessor(serviceBusOptions.Value.SplitQueueName);
        _function = function;
        _logger = logger;

        _processor.ProcessMessageAsync += HandleMessageAsync;
        _processor.ProcessErrorAsync += HandleErrorAsync;
    }

    // The CDP proxy requires authentication. WebProxy does not pick up the user:password
    // embedded in the proxy URL automatically, so the credentials must be set explicitly.
    private static WebProxy BuildWebProxy(string proxyAddress)
    {
        var proxyUri = new Uri(proxyAddress);
        var webProxy = new WebProxy($"{proxyUri.Scheme}://{proxyUri.Host}:{proxyUri.Port}");

        if (!string.IsNullOrEmpty(proxyUri.UserInfo))
        {
            var credentials = proxyUri.UserInfo.Split(':', 2);
            webProxy.Credentials = new NetworkCredential(
                Uri.UnescapeDataString(credentials[0]),
                credentials.Length > 1 ? Uri.UnescapeDataString(credentials[1]) : string.Empty);
        }

        return webProxy;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.StartProcessingAsync(stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken).ContinueWith(_ => { }, TaskScheduler.Default);
        await _processor.StopProcessingAsync(CancellationToken.None);
    }

    private async Task HandleMessageAsync(ProcessMessageEventArgs args)
    {
        _logger.LogInformation("Received message {MessageId}", args.Message.MessageId);

        var request = JsonSerializer.Deserialize<ProducerValidationInRequest>(args.Message.Body, JsonOptions);

        if (request is null)
        {
            _logger.LogError("Failed to deserialise message {MessageId}", args.Message.MessageId);
            await args.DeadLetterMessageAsync(args.Message, "DeserializationFailed", "Message body could not be deserialised");
            return;
        }

        await _function.RunAsync(request);
        await args.CompleteMessageAsync(args.Message);
    }

    private Task HandleErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus error on {EntityPath}", args.EntityPath);
        return Task.CompletedTask;
    }
}
