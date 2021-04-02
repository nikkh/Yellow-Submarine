using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Files.DataLake;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using YellowSubmarine.Common;

namespace YellowSubmarineFileAclHandler
{
    public class FileAclHandler
    {
        private readonly TelemetryClient telemetryClient;
        static readonly string drain = Environment.GetEnvironmentVariable("DRAIN").ToUpper();
        static readonly string skipResults = Environment.GetEnvironmentVariable("SkipResults").ToUpper();
        static readonly Uri serviceUri = new Uri(Environment.GetEnvironmentVariable("DataLakeUri"));
        static readonly string fileSystemName = Environment.GetEnvironmentVariable("FileSystemName");
        static readonly string dataLakeSasToken = Environment.GetEnvironmentVariable("DataLakeSasToken");

        static readonly DataLakeServiceClient serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(dataLakeSasToken));
        static readonly DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

        static readonly EventHubProducerClient inspectionResultClient =
            new EventHubProducerClient(Environment.GetEnvironmentVariable("ResultsEventHubFullConnectionString"),
                new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 0 } });


        readonly Metric eventHubBatchSize;
        readonly Metric eventHubBatchLatency;
        readonly Metric functionInvocations;
        readonly Metric messagesProcessed;


        public FileAclHandler(TelemetryConfiguration telemetryConfig)
        {
            telemetryClient = new TelemetryClient(telemetryConfig);
            eventHubBatchLatency = telemetryClient.GetMetric("New FileAcl Event Batch Latency");
            eventHubBatchSize = telemetryClient.GetMetric("New FileAcl Event Batch Size");
            functionInvocations = telemetryClient.GetMetric("New FileAcl Functions Invoked");
            messagesProcessed = telemetryClient.GetMetric("New FileAcl Messages Processed");
        }

        [FunctionName("FileAclHandler")]

        public async Task Run([EventHubTrigger("%FileAclHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log)
        {
            functionInvocations.TrackValue(1);
            if (drain == "TRUE")
            {
                return;
            }

            eventHubBatchSize.TrackValue(events.Length);
            var exceptions = new List<Exception>();
            double totalLatency = 0;
            EventDataBatch resultEventBatch = await inspectionResultClient.CreateBatchAsync();
            foreach (EventData eventData in events)
            {
                try
                {
                    DateTime enqueuedTimeUtc = eventData.EnqueuedTime.UtcDateTime;
                    var nowTimeUTC = DateTime.UtcNow;
                    totalLatency += nowTimeUTC.Subtract(enqueuedTimeUtc).TotalMilliseconds;
                    string messageBody = Encoding.UTF8.GetString(eventData.EventBody.ToArray());
                    DirectoryExplorationRequest dir = JsonConvert.DeserializeObject<DirectoryExplorationRequest>(messageBody);
                    telemetryClient.Context.GlobalProperties["RequestId"] = dir.RequestId;
                    
                    var fileClient = fileSystemClient.GetFileClient(dir.StartPath);
                    var aclResult = await fileClient.GetAccessControlAsync();
                    var fileResult = new ExplorationResult
                    {
                        Type = InspectionResultType.File,
                        Path = dir.StartPath,
                        Acls = JsonConvert.SerializeObject(aclResult.Value.AccessControlList),
                        RequestId = dir.RequestId,
                        ETag = dir.ETag,
                        ModifiedDateTime = dir.ModifiedDateTime,
                        Depth = dir.CurrentDepth
                    };
                    EventData fileEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(fileResult)));
                    if (!resultEventBatch.TryAdd(fileEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                    messagesProcessed.TrackValue(1);
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }
            if ((resultEventBatch.Count > 0) && (skipResults != "TRUE"))
            {
                await inspectionResultClient.SendAsync(resultEventBatch);
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
