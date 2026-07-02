using System.Net;
using System.Text.Json;
using Azure;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
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

    private readonly ServiceBusAdministrationClient _adminClient;
    private readonly ServiceBusProcessor _processor;
    private readonly ValidateProducerContentFunction _function;
    private readonly ILogger<ProducerValidationWorker> _logger;
    private readonly string _queueName;

    public ProducerValidationWorker(
        IOptions<ServiceBusOptions> serviceBusOptions,
        ValidateProducerContentFunction function,
        ILogger<ProducerValidationWorker> logger)
    {
        // CDP only allows outbound traffic via the Squid HTTP proxy, which cannot tunnel
        // raw AMQP over TCP (port 5671). AMQP over WebSockets runs on port 443 and can be
        // tunnelled through the proxy via HTTP CONNECT, so the client must be told to use it.
        var proxyAddress = Environment.GetEnvironmentVariable("CDP_HTTPS_PROXY")
            ?? Environment.GetEnvironmentVariable("HTTP_PROXY");
        var webProxy = string.IsNullOrWhiteSpace(proxyAddress) ? null : BuildWebProxy(proxyAddress);

        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };

        if (webProxy is not null)
        {
            clientOptions.WebProxy = webProxy;
        }

        var connectionString = serviceBusOptions.Value.ConnectionString;
        _queueName = serviceBusOptions.Value.SplitQueueName;

        var client = new ServiceBusClient(connectionString, clientOptions);
        _processor = client.CreateProcessor(_queueName);
        _adminClient = new ServiceBusAdministrationClient(connectionString, BuildAdminClientOptions(webProxy));
        _function = function;
        _logger = logger;

        _processor.ProcessMessageAsync += HandleMessageAsync;
        _processor.ProcessErrorAsync += HandleErrorAsync;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureQueueExistsAsync(stoppingToken);

        await _processor.StartProcessingAsync(stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken).ContinueWith(_ => { }, TaskScheduler.Default);
        await _processor.StopProcessingAsync(CancellationToken.None);
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

    // The administration client talks over HTTPS rather than AMQP, but it still needs to
    // be routed through the CDP proxy explicitly, using the same credentials as the data-plane client.
    private static ServiceBusAdministrationClientOptions BuildAdminClientOptions(WebProxy webProxy)
    {
        var options = new ServiceBusAdministrationClientOptions();

        if (webProxy is not null)
        {
            var httpClientHandler = new HttpClientHandler { Proxy = webProxy, UseProxy = true };
            options.Transport = new HttpClientTransport(httpClientHandler);
        }

        return options;
    }

    // Requires the connection to have "Manage" rights (e.g. the RootManageSharedAccessKey
    // policy). Mirrors the check-and-create pattern used by the payment facade CDP POC.
    private async Task EnsureQueueExistsAsync(CancellationToken cancellationToken)
    {
        if (await _adminClient.QueueExistsAsync(_queueName, cancellationToken))
        {
            _logger.LogInformation("Service Bus queue {QueueName} already exists", _queueName);
            return;
        }

        try
        {
            await _adminClient.CreateQueueAsync(_queueName, cancellationToken);
            _logger.LogInformation("Created Service Bus queue {QueueName}", _queueName);
        }
        catch (RequestFailedException ex) when (ex.Status == (int)HttpStatusCode.Conflict)
        {
            // Another instance created the queue between our existence check and create call.
            _logger.LogInformation("Service Bus queue {QueueName} was created concurrently by another instance", _queueName);
        }
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
