using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;
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
        readonly Metric blobsWritten;
        readonly Metric eventHubBatchLatency;
        readonly int maxThroughput;
        private static readonly CloudBlobClient blobClient = StorageAccount.NewFromConnectionString(Environment.GetEnvironmentVariable("OutputStorageConnection")).CreateCloudBlobClient();
        private static readonly string endpoint = Environment.GetEnvironmentVariable("CosmosEndPointUrl");
        private static readonly string cosmosMaxThroughput = Environment.GetEnvironmentVariable("CosmosMaxThroughput"); 
        private static readonly string authKey = Environment.GetEnvironmentVariable("CosmosAuthorizationKey");
        private static readonly CosmosClient cosmosClient = new CosmosClient(endpoint, authKey);
        private static readonly string cosmosDatabaseId = Environment.GetEnvironmentVariable("CosmosDatabaseId");
        private static readonly string cosmosContainerId = Environment.GetEnvironmentVariable("CosmosContainerId");
        private static readonly string useCosmos = Environment.GetEnvironmentVariable("UseCosmos");
        private readonly bool cosmosRequired = false;
        Container resultsContainer;
        CloudBlobContainer resultBlobContainer;
        private Database cosmosDb;
        public Persistor(TelemetryConfiguration telemetryConfig) 
        {
            maxThroughput = 400;
            if (!string.IsNullOrEmpty(cosmosMaxThroughput))
            {
                if (!int.TryParse(cosmosMaxThroughput, out int m)) maxThroughput = 400; else maxThroughput = m;
            }
            telemetryClient = new TelemetryClient(telemetryConfig);
            blobsWritten = telemetryClient.GetMetric("Explore Results Blobs Written");
            eventHubBatchLatency = telemetryClient.GetMetric("Explore Results Batch Latency");

            if (!string.IsNullOrEmpty(useCosmos)) 
            {
                if (useCosmos.ToUpper() == "TRUE") cosmosRequired = true;
            } 
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
                    if (cosmosRequired) await SaveToCosmosAsync(result);
                    await SaveToBlobAsync(result);
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

        private async Task SaveToBlobAsync(ExplorationResult result)
        {
            if (resultBlobContainer == null) resultBlobContainer = blobClient.GetContainerReference(result.RequestId);
            string extension = "file";
            if (result.Type == InspectionResultType.Directory) extension = "directory";
            var blobName = $"{result.RequestId}-{result.Path.Replace('/', '-')}.{extension}";
            var blob = resultBlobContainer.GetBlockBlobReference(blobName);
            var payload = JsonConvert.SerializeObject(result);
            MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(payload));
            await blob.UploadFromStreamAsync(stream);
        }

        private async Task SaveToCosmosAsync(ExplorationResult result)
        {
            cosmosDb = await cosmosClient.CreateDatabaseIfNotExistsAsync(cosmosDatabaseId);
            if (resultsContainer == null) 
            {
                ContainerProperties containerProperties = new ContainerProperties($"{cosmosContainerId}-{result.RequestId}", partitionKeyPath: "/PartitionKey");
                resultsContainer = await cosmosDb.CreateContainerIfNotExistsAsync(containerProperties, ThroughputProperties.CreateAutoscaleThroughput(maxThroughput));
            }
            await resultsContainer.CreateItemAsync(result, new PartitionKey(result.PartitionKey),
            new ItemRequestOptions()
            {
                EnableContentResponseOnWrite = false
            });
        }
    }
}
