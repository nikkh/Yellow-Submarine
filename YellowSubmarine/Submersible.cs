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
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using YellowSubmarine.Common;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Cosmos;

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
        static readonly EventHubClient inspectionRequestClient =
            EventHubClient.CreateFromConnectionString(
                Environment.GetEnvironmentVariable("RequestsEventHubFullConnectionString"));
        static readonly EventHubClient dirAclClientClient =
            EventHubClient.CreateFromConnectionString(
                Environment.GetEnvironmentVariable("DirAclEventHubFullConnectionString"));
        static readonly EventHubClient fileAclClient =
            EventHubClient.CreateFromConnectionString(
                Environment.GetEnvironmentVariable("FileAclEventHubFullConnectionString"));

        static Metric eventHubBatchSize;
        static Metric eventHubBatchLatency;
        static Metric functionInvocations;
        static Metric messagesProcessed;
        static Metric directoriesProcessed;
        static Metric filesProcessed;
        static Metric targetDepthAchieved;
        static Metric continuationPages;

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
            eventHubBatchSize = telemetryClient.GetMetric("New Explore Event Batch Size");
            eventHubBatchLatency = telemetryClient.GetMetric("New Explore Event Batch Latency");
            functionInvocations = telemetryClient.GetMetric("New Explore Functions Invoked");
            messagesProcessed = telemetryClient.GetMetric("New Explore Messages Processed");
            directoriesProcessed = telemetryClient.GetMetric("New Explore Directories Processed");
            filesProcessed = telemetryClient.GetMetric("New Explore Files Processed");
            targetDepthAchieved = telemetryClient.GetMetric("New Explore Target Depth Achieved");
            continuationPages = telemetryClient.GetMetric("New Explore Continuation Pages");
            if (!Int32.TryParse(defaultPageSize, out int ps)) pageSize = 5000; else pageSize = ps;
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
            string requestId = $"{Guid.NewGuid().ToString()}";
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
            EventData ed = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new DirectoryExplorationRequest { StartPath = startPath, RequestId=requestId, TargetDepth=targetDepth })));
            await inspectionRequestClient.SendAsync(ed);
            log.LogDebug($"{ec.FunctionName}: request to process directory {startPath} added to event hub {inspectionRequestClient.EventHubName}. Requestid: {requestId}");
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
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    var enqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc;
                    var nowTimeUTC = DateTime.UtcNow;
                    totalLatency += nowTimeUTC.Subtract(enqueuedTimeUtc).TotalMilliseconds;
                    DirectoryExplorationRequest dir = JsonConvert.DeserializeObject<DirectoryExplorationRequest>(messageBody);
                    
                    telemetryClient.Context.GlobalProperties["RequestId"] = dir.RequestId;
                    log.LogDebug($"{ec.FunctionName}: Processing event {j} for path {dir.StartPath} Requestid: {dir.RequestId}");
                    await InspectDirectory(dir, log, ec);
                    messagesProcessed.TrackValue(1);
                    await Task.Yield();
                    log.LogDebug($"{ec.FunctionName}: Finished processing {j} for path {dir.StartPath} Requestid: {dir.RequestId}");
                }
                catch (Exception e)
                {
                    telemetryClient.TrackException(e);
                    exceptions.Add(e);
                }
            }
            log.LogDebug($"{ec.FunctionName}: Finished processing {j} events for batch");
            eventHubBatchLatency.TrackValue(totalLatency / events.Length / 1000);

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private async Task InspectDirectory(DirectoryExplorationRequest dir, ILogger log, ExecutionContext ec)
        {
            
            log.LogDebug($"{ec.InvocationId}: inspecting directory {dir.StartPath} Requestid: {dir.RequestId}");
            var directoryClient = fileSystemClient.GetDirectoryClient(dir.StartPath);
            EventData directoryEvent;
            int itemsOnThisPage = 0;
            // if there is a continuation token, dont send the results again
            // they will have been sent on the first request
            if (string.IsNullOrEmpty(dir.ContinuationToken))
            {
                log.LogDebug($"{ec.FunctionName}: inspecting directory {dir.StartPath}. No continuation token found. Requestid: {dir.RequestId}");
                var directoryAclRequest = new DirectoryExplorationRequest()
                {
                     StartPath =  dir.StartPath,
                     RequestId = dir.RequestId, 
                     CurrentDepth = dir.CurrentDepth
                };
                // Send result for this directory
                directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(directoryAclRequest)));
                directoriesProcessed.TrackValue(1);
                if (skipDirAcls != "TRUE")
                { 
                    await dirAclClientClient.SendAsync(directoryEvent); 
                }
                log.LogDebug($"{ec.FunctionName}: {dir.StartPath} data sent to results. Requestid: {dir.RequestId}");
            }
            else
            {
                log.LogDebug($"{ec.FunctionName}: inspecting directory {dir.StartPath}. Continuation token {dir.ContinuationToken} Requestid: {dir.RequestId}");
                continuationPages.TrackValue(1);
            }
            // if target depth has been reached, stop.
            if (dir.CurrentDepth < dir.TargetDepth)
            {
                log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Current Depth {dir.CurrentDepth}, Target Depth {dir.TargetDepth}. Processing continues! Requestid: {dir.RequestId}");
                // Get directory contents and loop through them
                AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
                int i = 1;
                int currentPage = dir.PageNumber;
                // if we are supplied with a continuation token, use it.
                // this will read the page after the one that generated this token.
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

                EventDataBatch requestEventBatch = new EventDataBatch(pageSize * 1000);
                EventDataBatch fileAclEventBatch = new EventDataBatch(pageSize * 1000);
                string currentPageContinuation = "";
                
                await foreach (var page in pages)
                {
                    currentPage++;
                    itemsOnThisPage = 0;
                    log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Next Page has been read. Current page {currentPage}. Requestid: {dir.RequestId}");
                    currentPageContinuation = page.ContinuationToken;
                    log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. New continuation token is {currentPageContinuation}. ");
                    foreach (var pathItem in page.Values)
                    {
                        itemsOnThisPage++;

                        log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Processing {currentPage} #{i} Path {pathItem.Name}. Requestid: {dir.RequestId}");
                        // if it's a directory, just send a message to get it processed.
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
                            log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Found a sub-directory {pathItem.Name}.  Details are {payload}. Requestid: {dir.RequestId}");
                            directoryEvent = new EventData(Encoding.UTF8.GetBytes(payload)); 
                            if (!requestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                            log.LogDebug($"{ec.FunctionName}: request to process directory {pathItem.Name} added to event hub request batch. Requestid: {dir.RequestId}");
                        }
                        // if it's a file, get its acls
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
                            log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Found a file {pathItem.Name}.  Details are {messageString}. Requestid: {dir.RequestId}");
                            
                            EventData fileAclEvent = new EventData(Encoding.UTF8.GetBytes(messageString));
                            if (!fileAclEventBatch.TryAdd(fileAclEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                            log.LogDebug($"{ec.FunctionName}: request to process file {pathItem.Name} added to event hub fileAcl batch. Requestid: {dir.RequestId}");
                            filesProcessed.TrackValue(1);
                        }
                        i++;
                        log.LogDebug($"{ec.FunctionName}: done with {pathItem.Name} Requestid: {dir.RequestId}");
                        lastPathProcessed = pathItem.Name;
                    }
                    // We have processed this page and queued a request to process the next one - end execution
                    log.LogDebug($"{ec.FunctionName}: break at the end of page - other pages will be done by other invocations Requestid: {dir.RequestId}");
                    break;
                }

                // if we get here we have processed a full page
                // Send the batch (we might have processed only files - so check batch has some contents and send if it does.
                if (requestEventBatch.Count > 0)
                {
                    log.LogDebug($"{ec.FunctionName}: Sending batch of {requestEventBatch.Count} events to {inspectionRequestClient.EventHubName} Requestid: {dir.RequestId}");
                    await inspectionRequestClient.SendAsync(requestEventBatch);
                }
                
                if ((fileAclEventBatch.Count > 0) && (skipFileAcls != "TRUE"))
                {
                  log.LogDebug($"{ec.FunctionName}: Sending batch of {fileAclEventBatch.Count} events to {fileAclClient.EventHubName} Requestid: {dir.RequestId}");
                  await fileAclClient.SendAsync(fileAclEventBatch);
                }

                // if there is another page to come, place a record on queue to process it.
                if (!string.IsNullOrEmpty(currentPageContinuation))
                {
                   log.LogDebug($"{ec.FunctionName}: Another page to come for {dir.StartPath}. Will submit request to process it. {inspectionRequestClient.EventHubName} Requestid: {dir.RequestId}");
                    directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                    new DirectoryExplorationRequest
                    {
                        StartPath = dir.StartPath,
                        RequestId = dir.RequestId,
                        ContinuationToken = currentPageContinuation,
                        PageNumber = currentPage,
                        TargetDepth = dir.TargetDepth,
                        LastPathProcessed = lastPathProcessed

                    })));
                   // pathTrack.Add($"{ec.InvocationId}: processed {dir.LastPathProcessed}. Requesting new page.");
                   // log.LogWarning($"{dir.RequestId}::{ec.InvocationId}: dir={dir.StartPath}, processed {lastPathProcessed}. Requesting new page.");
                    await inspectionRequestClient.SendAsync(directoryEvent);
                    log.LogDebug($"{ec.FunctionName}: Sending page continuation request ({directoryEvent}, {currentPageContinuation}) event to {inspectionRequestClient.EventHubName} Requestid: {dir.RequestId}");
                }
                else
                {
                    log.LogWarning($"{dir.RequestId}::{ec.InvocationId}: dir={dir.StartPath}, page {currentPage} has {itemsOnThisPage} items");
                }
            }
            else 
            {
                
                log.LogDebug($"{ec.FunctionName}: directory {dir.StartPath}. Current Depth {dir.CurrentDepth}, Target Depth {dir.TargetDepth}. Processing Stops! Requestid: {dir.RequestId}");
                targetDepthAchieved.TrackValue(1);
            }
        }
    }
}
