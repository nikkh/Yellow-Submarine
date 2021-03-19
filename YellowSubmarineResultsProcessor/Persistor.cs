using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using YellowSubmarine.Common;

namespace YellowSubmarineResultsProcessor
{
    public class Persistor
    {
        private readonly TelemetryClient telemetryClient;
        private static readonly string outputContainerName = Environment.GetEnvironmentVariable("OutputStorageContainer");
        private static readonly CloudBlobClient blobClient = StorageAccount.NewFromConnectionString(Environment.GetEnvironmentVariable("OutputStorageConnection")).CreateCloudBlobClient();

        readonly Metric deepDiveResults;
        readonly CloudBlobContainer resultContainer;

        public Persistor(TelemetryConfiguration telemetryConfig) 
        {
            resultContainer = blobClient.GetContainerReference(outputContainerName);
            telemetryClient = new TelemetryClient(telemetryConfig);
            deepDiveResults = telemetryClient.GetMetric("DeepDiveResults");
        }

        [FunctionName("Persistor")]
       
        public async Task Run([EventHubTrigger("%ResultsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    deepDiveResults.TrackValue(1);
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    ExplorationResult result = JsonConvert.DeserializeObject<ExplorationResult>(messageBody);
                    string extension = "file";
                    if (result.Type == InspectionResultType.Directory) extension = "directory";
                    var blobName = $"{result.RequestId}-{result.Path.Replace('/', '-')}.{extension}";
                    var blob = resultContainer.GetBlockBlobReference(blobName);
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(messageBody));
                    await blob.UploadFromStreamAsync(stream);
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();


        }
    }
}