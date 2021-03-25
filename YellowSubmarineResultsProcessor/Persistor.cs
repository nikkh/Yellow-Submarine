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
        private static readonly CloudBlobClient blobClient = StorageAccount.NewFromConnectionString(Environment.GetEnvironmentVariable("OutputStorageConnection")).CreateCloudBlobClient();
        readonly Metric blobsWritten;
        readonly Metric eventHubBatchLatency;
        CloudBlobContainer resultContainer;

        public Persistor(TelemetryConfiguration telemetryConfig) 
        {
            telemetryClient = new TelemetryClient(telemetryConfig);
            blobsWritten = telemetryClient.GetMetric("Explore Results Blobs Written");
            eventHubBatchLatency = telemetryClient.GetMetric("Explore Results Batch Latency");
        }

        [FunctionName("Persistor")]
       
        public async Task Run([EventHubTrigger("%ResultsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            double totalLatency = 0;
            foreach (EventData eventData in events)
            {
                try
                {
                    var enqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc;
                    var nowTimeUTC = DateTime.UtcNow;
                    totalLatency += nowTimeUTC.Subtract(enqueuedTimeUtc).TotalMilliseconds;
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    ExplorationResult result = JsonConvert.DeserializeObject<ExplorationResult>(messageBody);
                    if (resultContainer == null) resultContainer = blobClient.GetContainerReference(result.RequestId);
                    string extension = "f";
                    if (result.Type == InspectionResultType.Directory) extension = "d";
                    var blobName = $"{result.Path.Replace('/', '-')}.{extension}";
                    var blob = resultContainer.GetBlockBlobReference(blobName);
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(messageBody));
                    await blob.UploadFromStreamAsync(stream);
                    blobsWritten.TrackValue(1);
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }
            eventHubBatchLatency.TrackValue(totalLatency / events.Length / 1000);

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();


        }
    }
}
