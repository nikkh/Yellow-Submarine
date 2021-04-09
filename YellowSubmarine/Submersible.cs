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
using System.Data.SqlClient;
using System.Data;
using System.Security.Cryptography;

namespace YellowSubmarine
{
    public class Submersible
    {
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
        static readonly EventHubProducerClient inspectionRequestClient =
            new EventHubProducerClient(Environment.GetEnvironmentVariable("RequestsEventHubFullConnectionString"),
                new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 3 } });
        static readonly EventHubProducerClient fileAclClient =
          new EventHubProducerClient(Environment.GetEnvironmentVariable("FileAclEventHubFullConnectionString"),
              new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 3 } });
        static readonly EventHubProducerClient dirAclClient =
           new EventHubProducerClient(Environment.GetEnvironmentVariable("DirAclEventHubFullConnectionString"),
               new EventHubProducerClientOptions { RetryOptions = new EventHubsRetryOptions { MaximumRetries = 3 } });

        static Metric eventHubBatchSize;
        static Metric eventHubBatchLatency;
        static Metric functionInvocations;
        static Metric messagesProcessed;
        static Metric directoriesProcessed;
        static Metric filesProcessed;
        static Metric filesSentToAclHandler;
        static Metric targetDepthAchieved;
        static Metric continuationPages;
        static string requestId;

        public Submersible(TelemetryConfiguration telemetryConfig) 
        {
            telemetryClient = new TelemetryClient(telemetryConfig);
            eventHubBatchSize = telemetryClient.GetMetric("Processor Event Batch Size");
            eventHubBatchLatency = telemetryClient.GetMetric("Processor Event Batch Latency");
            functionInvocations = telemetryClient.GetMetric("Processor Functions Invoked");
            messagesProcessed = telemetryClient.GetMetric("Processor Messages Processed", "RequestId");
            directoriesProcessed = telemetryClient.GetMetric("Processor Directories Processed", "RequestId");
            filesProcessed = telemetryClient.GetMetric("Processor Files Processed", "RequestId");
            filesSentToAclHandler = telemetryClient.GetMetric("Processor Files Sent to Acl Handler", "RequestId");
            targetDepthAchieved = telemetryClient.GetMetric("Processor Target Depth Achieved", "RequestId");
            continuationPages = telemetryClient.GetMetric("Processor Continuation Pages", "RequestId");
            if (!Int32.TryParse(defaultPageSize, out int ps)) pageSize = 2500; else pageSize = ps;
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

            
            requestId = $"{Guid.NewGuid().ToString()}";
            string tMessage = $"A deep dive into data lake {serviceUri} was requested. Exploration will start at path {parameters.StartPath}.  The tracking Id for your results is {requestId}";
            EventDataBatch inspectionRequestEventBatch = await inspectionRequestClient.CreateBatchAsync();
            var directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                    new DirectoryExplorationRequest { StartPath = startPath, RequestId = requestId, TargetDepth = targetDepth })));   
            if (!inspectionRequestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
            await inspectionRequestClient.SendAsync(inspectionRequestEventBatch);
            telemetryClient.TrackEvent("InspectionRunTriggered",
                          new Dictionary<string, string> { { "path", startPath }, { "requestId", requestId }, { "targetDepth", targetDepth.ToString() } });
            return new OkObjectResult(tMessage);
        }

        [FunctionName("Explore")]
        public async Task Explore([EventHubTrigger("%RequestsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log, ExecutionContext ec)
        {
            functionInvocations.TrackValue(1);
            if (drain == "TRUE")
            {
                log.LogDebug($"Draining in progress!");
                return;
            }
            double totalLatency = 0;
            var exceptions = new List<Exception>();
            eventHubBatchSize.TrackValue(events.Count());
            log.LogDebug($"{ec.FunctionName} Processing a batch of {events.Count()} events");
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
            EventDataBatch requestEventBatch = await inspectionRequestClient.CreateBatchAsync();
            EventDataBatch fileAclEventBatch = await fileAclClient.CreateBatchAsync();
            EventDataBatch dirAclEventBatch = await dirAclClient.CreateBatchAsync();

            // If this is the first page for this directory we can store the results
            if (string.IsNullOrEmpty(dir.ContinuationToken))
            {
                var messageString = JsonConvert.SerializeObject(dir);
                EventData dirAclEvent = new EventData(Encoding.UTF8.GetBytes(messageString));
                if (!dirAclEventBatch.TryAdd(dirAclEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                directoriesProcessed.TrackValue(1, dir.RequestId);
                log.LogDebug($"{ec.FunctionName} first page for directory {dir.StartPath} event queued for DirAcls to process");
            }
            else
            {
                continuationPages.TrackValue(1, dir.RequestId);
            }
            
            // Set pointer to next page
            var directoryClient = fileSystemClient.GetDirectoryClient(dir.StartPath);
            EventData directoryEvent;
            AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);

            int currentPage = dir.PageNumber;
            IAsyncEnumerable<Page<PathItem>> pages;
            if (string.IsNullOrEmpty(dir.ContinuationToken))
            {
                pages = pathItems.AsPages(null, pageSize);
            }
            else
            { 
                pages = pathItems.AsPages(dir.ContinuationToken, pageSize);
            }
            string lastPathProcessed = "";
            string currentPageContinuation = "";
            int dirs = 0; int files = 0;
            await foreach (var page in pages)
            {
                currentPage++;
                // check if this page has already been processed
                // If so we just need to stop
                if (await Utils.PageAlreadyProcessedAsync(dir.RequestId, dir.StartPath, currentPage))
                {
                    log.LogInformation($"Request: {dir.RequestId}, Path {dir.StartPath}, Page {currentPage} has already been processed.");
                    break;
                }
                // otherwise log it now to make sure
                else
                {
                    try
                    {
                        await Utils.LogPageCompletionAsync(dir.RequestId, dir.StartPath, currentPage);
                    }
                    catch (SqlException e)
                    {
                        log.LogInformation($"Unable to log completion for Request: {dir.RequestId}, Path {dir.StartPath}, Page {currentPage}. Processing aborted.");
                        break;
                    }
                }
                currentPageContinuation = page.ContinuationToken;
                foreach (var pathItem in page.Values)
                {
                    
                    if ((bool) pathItem.IsDirectory)
                    {
                        dirs++;
                        var subdirectoryRequest = JsonConvert.SerializeObject(
                                new DirectoryExplorationRequest
                                {
                                    StartPath = pathItem.Name,
                                    RequestId = dir.RequestId,
                                    TargetDepth = dir.TargetDepth,
                                    CurrentDepth = dir.CurrentDepth,
                                    ContinuationToken = null,
                                    PageNumber = 0,
                                    LastPathProcessed = null,
                                    ETag = pathItem.ETag.ToString(),
                                    ModifiedDateTime = pathItem.LastModified.UtcDateTime.ToString(),
                                });
                        directoryEvent = new EventData(Encoding.UTF8.GetBytes(subdirectoryRequest));
                        if (!requestEventBatch.TryAdd(directoryEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                    }
                    else
                    {
                        files++;
                        var fileAclRequest = new DirectoryExplorationRequest
                        {
                            CurrentDepth = dir.CurrentDepth,
                            StartPath = pathItem.Name,
                            RequestId = dir.RequestId,
                            ETag = pathItem.ETag.ToString(),
                            ModifiedDateTime = pathItem.LastModified.UtcDateTime.ToString(),
                        };
                        var messageString = JsonConvert.SerializeObject(fileAclRequest);
                        EventData fileAclEvent = new EventData(Encoding.UTF8.GetBytes(messageString));
                        if (!fileAclEventBatch.TryAdd(fileAclEvent)) throw new Exception("Maximum batch size of event hub batch exceeded!");
                        filesProcessed.TrackValue(1, dir.RequestId);
                    }
                    lastPathProcessed = pathItem.Name;
                }
                // We have processed this page and queued a request to process the next one - end execution
                break;
            }

            if (requestEventBatch.Count > 0)
            {
                log.LogDebug($"{ec.FunctionName} Directory {dir.StartPath}, page {currentPage}, Inspection Requests={requestEventBatch.Count}");
                await inspectionRequestClient.SendAsync(requestEventBatch);
            }
            if (fileAclEventBatch.Count > 0)
            {
                log.LogDebug($"{ec.FunctionName} Directory {dir.StartPath}, page {currentPage}, File Acl Requests={fileAclEventBatch.Count}");
                filesSentToAclHandler.TrackValue(fileAclEventBatch.Count, dir.RequestId);
                await fileAclClient.SendAsync(fileAclEventBatch);
            }
            if (dirAclEventBatch.Count > 0)
            {
                log.LogDebug($"{ec.FunctionName} Directory {dir.StartPath}, page {currentPage}, Dir Acl Requests={dirAclEventBatch.Count}");
                await dirAclClient.SendAsync(dirAclEventBatch);
            }


            log.LogDebug($"{ec.FunctionName} directory {dir.StartPath} page {currentPage} contains {dirs} subdirectories and {files} files");
            
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
            else 
            {
                log.LogDebug($"{ec.FunctionName} Processing Complete for Directory {dir.StartPath}");
            }
          
        }     
    }
}
