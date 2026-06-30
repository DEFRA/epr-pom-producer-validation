using MongoDB.Driver;

namespace EPR.ProducerContentValidation.Application.Utils.Mongo;

public interface IMongoDbClientFactory
{
    IMongoClient GetClient();

    IMongoCollection<T> GetCollection<T>(string collection);
}
