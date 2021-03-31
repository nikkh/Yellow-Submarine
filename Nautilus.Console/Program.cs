using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nautilus
{
    class Program
    {
        static readonly Uri serviceUri = new Uri(Environment.GetEnvironmentVariable("DataLakeUri"));
        static readonly string fileSystemName = Environment.GetEnvironmentVariable("FileSystemName");
        static readonly string dataLakeSasToken = Environment.GetEnvironmentVariable("DataLakeSasToken");
        static readonly DataLakeServiceClient serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(dataLakeSasToken));
        static readonly DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
        static async Task Main(string[] args)
        {
            string p1 = "parent1";
            string p2 = "parent2";
            string p3 = "parent3";
            var c1 = CountDirectoryItems(p1);
            Console.WriteLine("-------------------------------------------------------------");
            var c2 = CountDirectoryItems(p2);
            Console.WriteLine("-------------------------------------------------------------");
            var c3 = CountDirectoryItems(p3);
            Console.WriteLine("-------------------------------------------------------------");
            Console.WriteLine($"Directory {p1} has {c1} items");
            Console.WriteLine($"Directory {p2} has {c2} items");
            Console.WriteLine($"Directory {p3} has {c3} items");
        }

        private static async Task<int> CountDirectoryItems(string dir)
        {
            int count = 0; int i = 0;
            Console.WriteLine($"Counting items in directory {dir}");
            var directoryClient = fileSystemClient.GetDirectoryClient(dir);
            AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(true);
            IAsyncEnumerable<Page<PathItem>> pages;
            pages = pathItems.AsPages(null, 2000);
            await foreach (var page in pages)
            {
                i++;
                count += page.Values.Count;
                Console.WriteLine($"Processing page {i}, running total is {count}");
            }
            Console.WriteLine($"Complete.  There are {count} items");
            return count;
        }
    }
}
