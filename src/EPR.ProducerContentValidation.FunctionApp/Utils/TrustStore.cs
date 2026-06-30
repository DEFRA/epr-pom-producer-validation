using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;

namespace EPR.ProducerContentValidation.FunctionApp.Utils;

[ExcludeFromCodeCoverage]
public static class TrustStore
{
    public static void LoadCustomTrustStoreFromEnvironment(this IServiceCollection services)
    {
        var certificates = GetCertificates();
        AddCertificates(certificates);
    }

    private static List<byte[]> GetCertificates()
    {
        return Environment.GetEnvironmentVariables().Cast<DictionaryEntry>()
            .Where(entry =>
                entry.Key.ToString()!.StartsWith("TRUSTSTORE_", StringComparison.Ordinal) && IsBase64String(entry.Value?.ToString() ?? string.Empty))
            .Select(entry => Convert.FromBase64String(entry.Value?.ToString() ?? string.Empty)).ToList();
    }

    private static void AddCertificates(List<byte[]> certificates)
    {
        if (certificates.Count == 0)
        {
            return; // to stop trust store access denied issues on Macs
        }

        var loadedCertificates = certificates.Select(X509CertificateLoader.LoadCertificate);
        var certificateCollection = new X509Certificate2Collection();

        foreach (var certificate in loadedCertificates)
        {
            certificateCollection.Add(certificate);
        }

        var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
        try
        {
            store.Open(OpenFlags.ReadWrite);
            store.AddRange(certificateCollection);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Root certificate import failed: " + ex.Message, ex);
        }
        finally
        {
            store.Close();
        }
    }

    private static bool IsBase64String(string str)
    {
        var buffer = new Span<byte>(new byte[str.Length]);
        return Convert.TryFromBase64String(str, buffer, out _);
    }
}
