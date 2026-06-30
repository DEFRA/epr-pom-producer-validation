using MongoDB.Driver;
using MongoDB.Driver.Authentication.AWS;

namespace EPR.ProducerContentValidation.Application.Utils.Mongo;

public static class MongoExtensions
{
    private static int _initialized;

    public static void Register()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
        {
            return;
        }

        MongoClientSettings.Extensions.AddAWSAuthentication();
    }
}
