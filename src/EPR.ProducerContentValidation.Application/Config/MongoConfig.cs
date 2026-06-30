namespace EPR.ProducerContentValidation.Application.Config;

using System.ComponentModel.DataAnnotations;

public class MongoConfig
{
    public const string Section = "Mongo";

    [Required]
    public string DatabaseUri { get; set; }

    [Required]
    public string DatabaseName { get; set; }
}
