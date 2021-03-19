using Microsoft.ApplicationInsights.AspNetCore.Extensions;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;

[assembly: FunctionsStartup(typeof(YellowSubmarine.Startup))]
namespace YellowSubmarine
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {

            var localRoot = Environment.GetEnvironmentVariable("AzureWebJobsScriptRoot");
            var azureRoot = $"{Environment.GetEnvironmentVariable("HOME")}/site/wwwroot";
            var actualRoot = localRoot ?? azureRoot;

            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(actualRoot)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"settings.json", true, true)
                .AddEnvironmentVariables();
            IConfiguration configuration = configBuilder.Build();
            builder.Services.AddSingleton(configuration);

            var appInsightsKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY"); 
            var aiOptions = new ApplicationInsightsServiceOptions();
            aiOptions.InstrumentationKey = appInsightsKey;
            builder.Services.AddApplicationInsightsTelemetry(aiOptions);
        }
    }
}