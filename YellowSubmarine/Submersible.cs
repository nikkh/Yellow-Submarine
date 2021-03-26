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

namespace YellowSubmarine
{
    public class Submersible
    {
        private static readonly CloudBlobClient blobClient = StorageAccount.NewFromConnectionString(Environment.GetEnvironmentVariable("OutputStorageConnection")).CreateCloudBlobClient();
        private readonly TelemetryClient telemetryClient;
        static readonly string drain = Environment.GetEnvironmentVariable("DRAIN").ToUpper();
        static readonly Uri serviceUri = new Uri(Environment.GetEnvironmentVariable("DataLakeUri"));
        static readonly string fileSystemName = Environment.GetEnvironmentVariable("FileSystemName");
        static readonly string dataLakeSasToken = Environment.GetEnvironmentVariable("DataLakeSasToken");
        static readonly string defaultPageSize = Environment.GetEnvironmentVariable("PageSize");
        readonly int pageSize;

        static readonly DataLakeServiceClient serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(dataLakeSasToken));
        static readonly DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
       
        static readonly string requestsPath = Environment.GetEnvironmentVariable("RequestsHub");
        static readonly string resultsPath = Environment.GetEnvironmentVariable("ResultsHub");
        static readonly EventHubClient inspectionRequestClient =
            EventHubClient.CreateFromConnectionString(
                Environment.GetEnvironmentVariable("RequestsEventHubFullConnectionString"));
        static readonly EventHubClient inspectionResultClient =
            EventHubClient.CreateFromConnectionString(
                Environment.GetEnvironmentVariable("ResultsEventHubFullConnectionString"));
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
        }

        
        [FunctionName("Dive")]
        public async Task<IActionResult> Dive(
           [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ILogger log)
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

            telemetryClient.TrackEvent($"Deep Dive Request triggered by Http POST", new Dictionary<string, string>() { { "directory", startPath } });
            string requestId = $"{Guid.NewGuid().ToString()}";
            string tMessage = $"A deep dive into data lake {serviceUri} was requested. Exploration will start at path {parameters.StartPath}.  The tracking Id for your results is {requestId}";
            var outputContainer = blobClient.GetContainerReference(requestId);
            await outputContainer.CreateIfNotExistsAsync();
            EventData ed = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new DirectoryExplorationRequest { StartPath = startPath, RequestId=requestId, TargetDepth=targetDepth })));
            await inspectionRequestClient.SendAsync(ed);
            return new OkObjectResult(tMessage);
        }

        [FunctionName("Explore")]
        public async Task Explore([EventHubTrigger("%RequestsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log)
        {
            functionInvocations.TrackValue(1);
            if (drain == "TRUE")
            {
                return;
            }

            double totalLatency = 0;
            var exceptions = new List<Exception>();
            eventHubBatchSize.TrackValue(events.Count());
            foreach (EventData eventData in events)
            {
                try
                {
                    
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    var enqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc;
                    var nowTimeUTC = DateTime.UtcNow;
                    totalLatency += nowTimeUTC.Subtract(enqueuedTimeUtc).TotalMilliseconds;
                    DirectoryExplorationRequest dir = JsonConvert.DeserializeObject<DirectoryExplorationRequest>(messageBody);
                    await InspectDirectory(dir, log);
                    messagesProcessed.TrackValue(1);
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

        private async Task InspectDirectory(DirectoryExplorationRequest dir, ILogger log)
        {

            var directoryClient = fileSystemClient.GetDirectoryClient(dir.StartPath);
            EventData directoryEvent;
            Response<PathAccessControl> aclResult;

            // if there is a continuation token, dont send the results again
            // they will have been sent on the first request
            if (string.IsNullOrEmpty(dir.ContinuationToken))
            {
                // Get ACL for this directory
                var directoryProps = await directoryClient.GetPropertiesAsync();
                aclResult = await directoryClient.GetAccessControlAsync();
                var directoryResult = new ExplorationResult
                {
                    Type = InspectionResultType.Directory,
                    Path = dir.StartPath,
                    Acls = JsonConvert.SerializeObject(aclResult.Value.AccessControlList),
                    RequestId = dir.RequestId,
                    ETag = directoryProps.Value.ETag.ToString(),
                    ModifiedDateTime = directoryProps.Value.LastModified.UtcDateTime.ToString(),
                    Depth = dir.CurrentDepth
                };

                // Send result for this directory
                directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(directoryResult)));
                directoriesProcessed.TrackValue(1);
                await inspectionResultClient.SendAsync(directoryEvent);
            }
            else
            {
                continuationPages.TrackValue(1);
            }
            // if target depth has been reached, stop.
            if (dir.CurrentDepth < dir.TargetDepth)
            {
                // Get directory contents and loop through them
                AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
                int i = 1;
                int currentPage = dir.PageNumber;
                // if we are supplied with a continuation token, use it.
                // this will read the page after the one that generated this token.
                IAsyncEnumerable<Page<PathItem>> pages;
                if (string.IsNullOrEmpty(dir.ContinuationToken))
                {
                    pages = pathItems.AsPages(null, pageSize);
                }
                else
                {
                    pages = pathItems.AsPages(dir.ContinuationToken, pageSize);
                }
                EventDataBatch requestEventBatch = new EventDataBatch(pageSize * 1000);
                EventDataBatch resultEventBatch = new EventDataBatch(pageSize * 1000);
                EventDataBatch fileAclEventBatch = new EventDataBatch(pageSize * 1000);
                string currentPageContinuation = "";
                await foreach (var page in pages)
                {
                    currentPage++;
                    currentPageContinuation = page.ContinuationToken;
                    foreach (var pathItem in page.Values)
                    {
                        // if it's a directory, just send a message to get it processed.
                        if ((bool)pathItem.IsDirectory)
                        {
                            directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                                new DirectoryExplorationRequest
                                {
                                    StartPath = pathItem.Name,
                                    RequestId = dir.RequestId,
                                    TargetDepth = dir.TargetDepth,
                                    CurrentDepth = dir.CurrentDepth + 1
                                }))
                            ); 
                            if (!requestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
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
                            log.LogInformation($"YYYY> MessageString is {messageString}");
                            EventData fileAclEvent = new EventData(Encoding.UTF8.GetBytes(messageString));
                            if (!fileAclEventBatch.TryAdd(fileAclEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                            filesProcessed.TrackValue(1);
                        }
                        i++;
                    }
                    // We have processed this page and queued a request to process the next one - end execution
                    break;
                }

                // if we get here we have processed a full page
                // Send the batch (we might have processed only files - so check batch has some contents and send if it does.
                if (requestEventBatch.Count > 0)
                {
                    await inspectionRequestClient.SendAsync(requestEventBatch);
                }
                if (resultEventBatch.Count > 0)
                {
                    await inspectionResultClient.SendAsync(resultEventBatch);
                }
                if (fileAclEventBatch.Count > 0)
                {
                    await fileAclClient.SendAsync(fileAclEventBatch);
                }

                // if there is another page to come, place a record on queue to process it.
                if (!string.IsNullOrEmpty(currentPageContinuation))
                {
                    directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                    new DirectoryExplorationRequest
                    {
                        StartPath = dir.StartPath,
                        RequestId = dir.RequestId,
                        ContinuationToken = currentPageContinuation,
                        PageNumber = currentPage
                    }))); ;
                    await inspectionRequestClient.SendAsync(directoryEvent);
                }
            }
            else 
            {
                targetDepthAchieved.TrackValue(1);
            }
        }
    }
}
