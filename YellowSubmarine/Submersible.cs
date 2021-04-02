using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using YellowSubmarine.Common;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Storage.Queue;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;

namespace YellowSubmarine
{
    public class Submersible
    {
        
        private static readonly CloudBlobClient blobClient = StorageAccount.NewFromConnectionString(Environment.GetEnvironmentVariable("OutputStorageConnection")).CreateCloudBlobClient();
        
        private readonly TelemetryClient telemetryClient;
        static readonly string drain = Environment.GetEnvironmentVariable("DRAIN").ToUpper();

        static readonly string skipFileAcls = Environment.GetEnvironmentVariable("SkipFileAcls").ToUpper();
        static readonly string skipDirAcls = Environment.GetEnvironmentVariable("SkipDirAcls").ToUpper();


        static readonly Uri serviceUri = new Uri(Environment.GetEnvironmentVariable("DataLakeUri"));
        static readonly string fileSystemName = Environment.GetEnvironmentVariable("FileSystemName");
        static readonly string dataLakeSasToken = Environment.GetEnvironmentVariable("DataLakeSasToken");
        static readonly string defaultPageSize = Environment.GetEnvironmentVariable("PageSize");
        readonly int pageSize;

        static readonly DataLakeServiceClient serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(dataLakeSasToken));
        static readonly DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
       
        static readonly string requestsPath = Environment.GetEnvironmentVariable("RequestsHub");


        
        static readonly EventHubProducerClient inspectionRequestClient =
            new EventHubProducerClient(Environment.GetEnvironmentVariable("RequestsEventHubFullConnectionString"),
                new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 0 } });

        static readonly EventHubProducerClient dirAclClient =
           new EventHubProducerClient(Environment.GetEnvironmentVariable("DirAclEventHubFullConnectionString"),
               new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 0 } });

        static readonly EventHubProducerClient fileAclClient =
          new EventHubProducerClient(Environment.GetEnvironmentVariable("FileAclEventHubFullConnectionString"),
              new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 0 } });

        
        static Metric eventHubBatchSize;
        static Metric eventHubBatchLatency;
        static Metric functionInvocations;
        static Metric messagesProcessed;
        static Metric directoriesProcessed;
        static Metric filesProcessed;
        static Metric targetDepthAchieved;
        static Metric continuationPages;

        static string requestId;

        private static readonly string endpoint = Environment.GetEnvironmentVariable("CosmosEndPointUrl");
        private static readonly string cosmosMaxThroughput = Environment.GetEnvironmentVariable("CosmosMaxThroughput");
        private static readonly string authKey = Environment.GetEnvironmentVariable("CosmosAuthorizationKey");
        private static readonly CosmosClient cosmosClient = new CosmosClient(endpoint, authKey);
        private static readonly string cosmosDatabaseId = Environment.GetEnvironmentVariable("CosmosDatabaseId");
        private static readonly string cosmosContainerId = Environment.GetEnvironmentVariable("CosmosContainerId");
        private static readonly string useCosmos = Environment.GetEnvironmentVariable("UseCosmos");
        private readonly bool cosmosRequired = false;
        Container resultsCosmosContainer;
        private Database cosmosDb;
        readonly int maxThroughput;

        
        public Submersible(TelemetryConfiguration telemetryConfig) 
        {
            
            telemetryClient = new TelemetryClient(telemetryConfig);
            eventHubBatchSize = telemetryClient.GetMetric("Processor Event Batch Size");
            eventHubBatchLatency = telemetryClient.GetMetric("Processor Event Batch Latency");
            functionInvocations = telemetryClient.GetMetric("Processor Functions Invoked");
            messagesProcessed = telemetryClient.GetMetric("Processor Messages Processed");
            directoriesProcessed = telemetryClient.GetMetric("Processor Directories Processed");
            filesProcessed = telemetryClient.GetMetric("Processor Files Processed");
            targetDepthAchieved = telemetryClient.GetMetric("Processor Target Depth Achieved");
            continuationPages = telemetryClient.GetMetric("Processor Continuation Pages");
            if (!Int32.TryParse(defaultPageSize, out int ps)) pageSize = 2500; else pageSize = ps;
            maxThroughput = 400;
            if (!string.IsNullOrEmpty(cosmosMaxThroughput))
            {
                if (!int.TryParse(cosmosMaxThroughput, out int m)) maxThroughput = 400; else maxThroughput = m;
            }
            if (!string.IsNullOrEmpty(useCosmos))
            {
                if (useCosmos.ToUpper() == "TRUE") cosmosRequired = true;
            }
        }

        
        [FunctionName("Dive")]
        public async Task<IActionResult> Dive(
           [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ILogger log, ExecutionContext ec)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic parameters = JsonConvert.DeserializeObject(requestBody);

            string startPath = parameters.StartPath;
            if (string.IsNullOrEmpty(startPath)) 
                throw new Exception("Start Path was not specified");

            int targetDepth = int.MaxValue;
            string targetDepthParameter = parameters.TargetDepth;
            if (!string.IsNullOrEmpty(targetDepthParameter)) 
            {
                if (!int.TryParse(parameters.TargetDepth.ToString(), out int td)) targetDepth = int.MaxValue; else targetDepth = td;
            }

            telemetryClient.TrackEvent($"Directory Inspection was triggered by Http POST", new Dictionary<string, string>() { { "directory", startPath } });
            requestId = $"{Guid.NewGuid().ToString()}";
            string tMessage = $"A deep dive into data lake {serviceUri} was requested. Exploration will start at path {parameters.StartPath}.  The tracking Id for your results is {requestId}";
            var outputContainer = blobClient.GetContainerReference(requestId);
            await outputContainer.CreateIfNotExistsAsync();
            log.LogDebug($"{ec.FunctionName}: Storage container {outputContainer.Uri} was created for results");
            


            if (cosmosRequired)
            {
                cosmosDb = await cosmosClient.CreateDatabaseIfNotExistsAsync(cosmosDatabaseId);
                if (resultsCosmosContainer == null)
                {
                    ContainerProperties containerProperties = new ContainerProperties($"{cosmosContainerId}-{requestId}", partitionKeyPath: "/PartitionKey");
                    resultsCosmosContainer = await cosmosDb.CreateContainerIfNotExistsAsync(containerProperties, ThroughputProperties.CreateAutoscaleThroughput(maxThroughput));
                }
                log.LogDebug($"{ec.FunctionName}: Cosmos Output is configured {resultsCosmosContainer.Id} was created for results in database {resultsCosmosContainer.Database.Id}");
            }


            EventDataBatch inspectionRequestEventBatch = await inspectionRequestClient.CreateBatchAsync();
            var directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                    new DirectoryExplorationRequest { StartPath = startPath, RequestId = requestId, TargetDepth = targetDepth })));   
            if (!inspectionRequestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
            await inspectionRequestClient.SendAsync(inspectionRequestEventBatch);
            return new OkObjectResult(tMessage);
        }

        [FunctionName("Explore")]
        public async Task Explore([EventHubTrigger("%RequestsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log, ExecutionContext ec)
        {
            functionInvocations.TrackValue(1);
            if (drain == "TRUE")
            {
                return;
            }
            
            double totalLatency = 0;
            var exceptions = new List<Exception>();
            eventHubBatchSize.TrackValue(events.Count());
            int j = 0;
            foreach (EventData eventData in events)
            {
                try
                {
                    j++;
                    string messageBody = Encoding.UTF8.GetString(eventData.EventBody.ToArray());
                    DateTime enqueuedTimeUtc = eventData.EnqueuedTime.UtcDateTime;
                    var nowTimeUTC = DateTime.UtcNow;
                    totalLatency += nowTimeUTC.Subtract(enqueuedTimeUtc).TotalMilliseconds;
                    var dir = JsonConvert.DeserializeObject<DirectoryExplorationRequest>(messageBody);
                    await InspectDirectoryAsync(dir, log, ec);
                    messagesProcessed.TrackValue(1, dir.RequestId);
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    telemetryClient.TrackException(e);
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

        private async Task InspectDirectoryAsync(DirectoryExplorationRequest dir, ILogger log, ExecutionContext ec)
        {
            
            log.LogDebug($"{ec.InvocationId}: inspecting directory {dir.StartPath} Requestid: {dir.RequestId}");
            var directoryClient = fileSystemClient.GetDirectoryClient(dir.StartPath);
            EventData directoryEvent;
            int itemsOnThisPage = 0;

            // If this is the first page for this directory we can queue arequest to get it's acls
            if (string.IsNullOrEmpty(dir.ContinuationToken))
            {
                await QueueDirAclRequestAsync(dir);
            }
            else
            {
                continuationPages.TrackValue(1);
            }
            // if target depth has been reached, stop.
            if (dir.CurrentDepth < dir.TargetDepth)
            {

                AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
                int i = 1;
                int currentPage = dir.PageNumber;
               
                IAsyncEnumerable<Page<PathItem>> pages;
                if (string.IsNullOrEmpty(dir.ContinuationToken))
                {
                    log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. No continuation token, current page {currentPage}. Requestid: {dir.RequestId}");
                    pages = pathItems.AsPages(null, pageSize);
                }
                else
                {
                    log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Continuation token {dir.ContinuationToken}, current page {currentPage}. Requestid: {dir.RequestId}");
                    pages = pathItems.AsPages(dir.ContinuationToken, pageSize);
                }
                string lastPathProcessed = "";


                EventDataBatch requestEventBatch = await inspectionRequestClient.CreateBatchAsync();
                EventDataBatch fileAclEventBatch = await fileAclClient.CreateBatchAsync();

                string currentPageContinuation = "";
                
                await foreach (var page in pages)
                {
                    currentPage++;
                    itemsOnThisPage = 0;
                    currentPageContinuation = page.ContinuationToken;
                    foreach (var pathItem in page.Values)
                    {
                        itemsOnThisPage++;
                        if ((bool)pathItem.IsDirectory)
                        {
                            var payload = JsonConvert.SerializeObject(
                                new DirectoryExplorationRequest
                                {
                                    StartPath = pathItem.Name,
                                    RequestId = dir.RequestId,
                                    TargetDepth = dir.TargetDepth,
                                    CurrentDepth = dir.CurrentDepth,
                                    ContinuationToken = null,
                                    PageNumber = 0,
                                    LastPathProcessed = null
                                });
                            directoryEvent = new EventData(Encoding.UTF8.GetBytes(payload)); 
                            if (!requestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                           
                        }
                        
                        else
                        {
                            var fileResult = new DirectoryExplorationRequest
                            {
                                CurrentDepth = dir.CurrentDepth,
                                StartPath = pathItem.Name, 
                                RequestId = dir.RequestId,
                                ETag = pathItem.ETag.ToString(),
                                ModifiedDateTime = pathItem.LastModified.UtcDateTime.ToString(),
                                
                            };
                            var messageString = JsonConvert.SerializeObject(fileResult);
                            EventData fileAclEvent = new EventData(Encoding.UTF8.GetBytes(messageString));
                            if (!fileAclEventBatch.TryAdd(fileAclEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                            filesProcessed.TrackValue(1, dir.RequestId);
                        }
                        i++;
                        lastPathProcessed = pathItem.Name;
                   }
                   // We have processed this page and queued a request to process the next one - end execution
                   break;
                }

                // if we get here we have processed a full page
                if (requestEventBatch.Count > 0)
                {
                    await inspectionRequestClient.SendAsync(requestEventBatch);
                }
                
                if ((fileAclEventBatch.Count > 0) && (skipFileAcls != "TRUE"))
                {
                  await fileAclClient.SendAsync(fileAclEventBatch);
                }

                // if there is another page to come, place a record on queue to process it.
                if (!string.IsNullOrEmpty(currentPageContinuation))
                { 
                    EventDataBatch inspectionRequestEventBatch = await inspectionRequestClient.CreateBatchAsync();
                    directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                            new DirectoryExplorationRequest
                            {
                                StartPath = dir.StartPath,
                                RequestId = dir.RequestId,
                                ContinuationToken = currentPageContinuation,
                                PageNumber = currentPage,
                                TargetDepth = dir.TargetDepth,
                                LastPathProcessed = lastPathProcessed

                            }
                            )));
                    if (!inspectionRequestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                    await inspectionRequestClient.SendAsync(inspectionRequestEventBatch);                   
                }
            }
            else 
            {
                targetDepthAchieved.TrackValue(1);
            }
        }

        private async Task QueueDirAclRequestAsync(DirectoryExplorationRequest dir)
        {
            EventDataBatch dirAclEventBatch = await dirAclClient.CreateBatchAsync();
            var directoryAclRequest = new DirectoryExplorationRequest()
            {
                StartPath = dir.StartPath,
                RequestId = dir.RequestId,
                CurrentDepth = dir.CurrentDepth
            };
            
            directoriesProcessed.TrackValue(1, dir.RequestId);
            if (skipDirAcls != "TRUE")
            {
                var directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(directoryAclRequest)));
                if (!dirAclEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                await dirAclClient.SendAsync(dirAclEventBatch);
            }
        }
    }
}
