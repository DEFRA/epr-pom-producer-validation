using System.Linq;
using EPR.ProducerContentValidation.Application.Models;
using EPR.ProducerContentValidation.Application.Options;
using EPR.ProducerContentValidation.Application.Services.Interfaces;
using EPR.ProducerContentValidation.Application.Utils.Mongo;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace EPR.ProducerContentValidation.Application.Services;

public class IssueCountService : IIssueCountService
{
    private const string CollectionName = "issueCounts";

    private readonly ValidationOptions _validationOptions;
    private readonly IMongoCollection<IssueCountDocument> _collection;

    public IssueCountService(
        IMongoDbClientFactory mongoDbClientFactory,
        IOptions<ValidationOptions> validationOptions)
    {
        _validationOptions = validationOptions.Value;
        _collection = mongoDbClientFactory.GetCollection<IssueCountDocument>(CollectionName);
    }

    public async Task IncrementIssueCountAsync(string key, int count)
    {
        var filter = Builders<IssueCountDocument>.Filter.Eq(x => x.Id, key);
        var update = Builders<IssueCountDocument>.Update.Inc(x => x.Count, count);

        await _collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true });
    }

    public async Task<int> GetRemainingIssueCapacityAsync(string key)
    {
        var currentCount = await FetchStoredCount(key);
        var remaining = _validationOptions.MaxIssuesToProcess - currentCount;
        return remaining <= 0 ? 0 : remaining;
    }

    private async Task<int> FetchStoredCount(string key)
    {
        var filter = Builders<IssueCountDocument>.Filter.Eq(x => x.Id, key);

        using var cursor = await _collection.FindAsync(filter);
        var documents = await cursor.ToListAsync();

        return documents.FirstOrDefault()?.Count ?? 0;
    }
}
