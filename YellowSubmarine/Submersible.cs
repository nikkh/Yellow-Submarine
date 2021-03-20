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

namespace YellowSubmarine
{
    public class Submersible
    {
        private readonly TelemetryClient telemetryClient;
        static readonly Uri serviceUri = new Uri(Environment.GetEnvironmentVariable("DataLakeUri"));
        static readonly string fileSystemName = Environment.GetEnvironmentVariable("FileSystemName");
        static readonly string dataLakeSasToken = Environment.GetEnvironmentVariable("DataLakeSasToken");
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

        readonly Metric deepDives;
        readonly Metric directoryInspectionRequests;
        readonly Metric directoryAclRequests;
        readonly Metric fileAclRequests;
        readonly Metric directoryPathRequests;
        readonly Metric directoryInspectionRequestsDurationMs;
        readonly Metric eventHubBatchSize;
        readonly Metric directoryPathItems;
        readonly Metric eventDeserializationTimeinMs;

        public Submersible(TelemetryConfiguration telemetryConfig) 
        {
            telemetryClient = new TelemetryClient(telemetryConfig);
            deepDives = telemetryClient.GetMetric("YSDeepDives");
            directoryInspectionRequests = telemetryClient.GetMetric("YSDirectoryInspectionRequests");
            directoryAclRequests = telemetryClient.GetMetric("YSDirectoryAclRequests");
            fileAclRequests = telemetryClient.GetMetric("YSFileAclRequests");
            directoryPathRequests = telemetryClient.GetMetric("YSDirectoryPathRequests");
            directoryInspectionRequestsDurationMs = telemetryClient.GetMetric("YSDirectoryInspectionRequestsDurationMs");
            eventHubBatchSize = telemetryClient.GetMetric("YSEventBatchSize");
            directoryPathItems = telemetryClient.GetMetric("YSDirectoryPathItems");
            eventDeserializationTimeinMs = telemetryClient.GetMetric("YSEventDeserializationTimeinMs");
        }

        
        [FunctionName("Dive")]
        public async Task<IActionResult> Dive(
           [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
           ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic parameters = JsonConvert.DeserializeObject(requestBody);

            string startPath = parameters.StartPath;
            if (string.IsNullOrEmpty(startPath)) 
                throw new Exception("Start Path was not specified");
            
            log.LogInformation($"Data Lake Exploration Trigger was received for path {parameters.StartPath}");
            telemetryClient.TrackEvent($"Deep Dive Request triggered by Http POST", new Dictionary<string, string>() { { "directory", startPath } });
            deepDives.TrackValue(1);
            string requestId = Guid.NewGuid().ToString();
            string tMessage = $"A deep dive into data lake {serviceUri} was requested. Exploration will start at path {parameters.StartPath}.  The tracking Id for your results is {requestId}";
            log.LogInformation(tMessage);
            EventData ed = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new DirectoryExplorationRequest { StartPath = startPath, RequestId=requestId })));
            await inspectionRequestClient.SendAsync(ed);
            return new OkObjectResult(tMessage);
        }

        [FunctionName("Explore")]
        public async Task Explore([EventHubTrigger("%RequestsHub%", Connection = "EventHubConnection")] EventData[] events, ILogger log)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            var exceptions = new List<Exception>();
            log.LogDebug($"Processing a batch of {events.Count()} requests");
            eventHubBatchSize.TrackValue(events.Count());
            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    var s = new Stopwatch();
                    s.Start();
                    DirectoryExplorationRequest dir = JsonConvert.DeserializeObject<DirectoryExplorationRequest>(messageBody);
                    s.Stop();
                    eventDeserializationTimeinMs.TrackValue(s.ElapsedMilliseconds);
                    log.LogDebug($"Event Request Id: {dir.RequestId}, Path={dir.StartPath} in {s.ElapsedMilliseconds} ms.");
                    await InspectDirectory(dir, log);
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    telemetryClient.TrackException(e);
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
            watch.Stop();
            directoryInspectionRequestsDurationMs.TrackValue(watch.ElapsedMilliseconds);
        }

        private async Task InspectDirectory(DirectoryExplorationRequest dir, ILogger log)
        {
            directoryInspectionRequests.TrackValue(1);
            // Get ACL for this directory
            var directoryClient = fileSystemClient.GetDirectoryClient(dir.StartPath);
            var directoryProps = await directoryClient.GetPropertiesAsync();
            var aclResult = await directoryClient.GetAccessControlAsync();
            directoryAclRequests.TrackValue(1);
            var directoryResult = new ExplorationResult {
                Type = InspectionResultType.Directory,
                Path = dir.StartPath,
                Acls = JsonConvert.SerializeObject(aclResult.Value.AccessControlList),
                RequestId = dir.RequestId, 
                ETag  = directoryProps.Value.ETag.ToString(), 
                ModifiedDateTime = directoryProps.Value.LastModified.UtcDateTime.ToString()
            };

            // Send result for this directory
            EventData directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(directoryResult)));
            await inspectionResultClient.SendAsync(directoryEvent);
            log.LogDebug($"Results for Path={dir.StartPath} sent to {resultsPath}");


            // Get directory contents and loop through them
            AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
            log.LogDebug($"Directory contents retrieved for {dir.StartPath}");
            directoryPathRequests.TrackValue(1);
            int i = 1;
            await foreach (var pathItem in pathItems)
            {
                log.LogDebug($"# {i}: {pathItem.Name} (IsDirectory={pathItem.IsDirectory})");
                // if it's a directory, just send a message to get it processed.
                if ((bool)pathItem.IsDirectory)
                {
                    directoryEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                        new DirectoryExplorationRequest { 
                            StartPath = pathItem.Name, 
                            RequestId=dir.RequestId }))
                        );
                    await inspectionRequestClient.SendAsync(directoryEvent);
                    log.LogDebug($"# {i}: {pathItem.Name}.  A request was queued to process this directory");
                }
                // if it's a file, get its acls
                else
                {
                    var fileClient = fileSystemClient.GetFileClient(pathItem.Name);
                    aclResult = await fileClient.GetAccessControlAsync();
                    fileAclRequests.TrackValue(1);
                    var fileResult = new ExplorationResult
                    {
                        Type = InspectionResultType.File,
                        Path = pathItem.Name,
                        Acls = JsonConvert.SerializeObject(aclResult.Value.AccessControlList),
                        RequestId = dir.RequestId,
                        ETag = pathItem.ETag.ToString(),
                        ModifiedDateTime = pathItem.LastModified.UtcDateTime.ToString()
                    };
                    EventData fileEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(fileResult)));
                    await inspectionResultClient.SendAsync(fileEvent);
                    log.LogDebug($"Results for File {fileResult.Path} sent to {resultsPath}");
                }
                i++;
            }
            directoryPathItems.TrackValue(i);
        }
    }
}
