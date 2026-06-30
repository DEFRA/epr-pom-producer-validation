using MongoDB.Bson.Serialization.Attributes;

namespace EPR.ProducerContentValidation.Application.Models;

public class IssueCountDocument
{
    [BsonId]
    public string Id { get; set; }

    public int Count { get; set; }
}
