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
        var client = new ServiceBusClient(serviceBusOptions.Value.ConnectionString);
        _processor = client.CreateProcessor(serviceBusOptions.Value.SplitQueueName);
        _function = function;
        _logger = logger;

        _processor.ProcessMessageAsync += HandleMessageAsync;
        _processor.ProcessErrorAsync += HandleErrorAsync;
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
