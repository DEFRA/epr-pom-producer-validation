using MongoDB.Bson.Serialization.Conventions;

namespace EPR.ProducerContentValidation.Application.Utils.Mongo;

public static class MongoConventions
{
    private static int _initialized;

    public static void Register()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
        {
            return;
        }

        var conventions = new ConventionPack
        {
            new CamelCaseElementNameConvention()
        };

        ConventionRegistry.Register("CamelCase", conventions, _ => true);
    }
}
