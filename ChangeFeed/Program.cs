using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace ChangeFeed
{
    using System.Threading;

    class Program
    {
        static void Main(string[] args)
        {
            var collection = "deleteditems";
            var ttl = 60;
            //DocumentClientHelper.CreateCollection(collection).Wait();
            DocumentClientHelper.AddItem(collection, "first").Wait();
            Thread.Sleep((ttl + 1) * 1000);
            DocumentClientHelper.DeleteItem(collection, "first").Wait();
        }

        private static async Task ProcessChangeFeed()
        {
            var dbName = ConfigurationManager.AppSettings["db"];
            var collectionName = ConfigurationManager.AppSettings["collection"];
            var endpointUrl = ConfigurationManager.AppSettings["endpoint"];
            var authorizationKey = ConfigurationManager.AppSettings["authKey"];

            var collectionUri = UriFactory.CreateDocumentCollectionUri(dbName, collectionName);

            var checkpoints = new Dictionary<string, string>();

            using (var client = new DocumentClient(
                new Uri(endpointUrl),
                authorizationKey,
                new ConnectionPolicy
                    {
                        ConnectionMode = ConnectionMode.Direct,
                        ConnectionProtocol = Protocol.Tcp
                    }))
            {
                var changes = GetChanges(client, collectionUri, checkpoints).Result;
            }
        }

        private static async Task<Dictionary<string, string>> GetChanges(
            DocumentClient client,
            Uri collectionUri,
            Dictionary<string, string> checkpoints)
        {
            string pkRangesResponseContinuation = null;
            var partitionKeyRanges = new List<PartitionKeyRange>();

            do
            {
                FeedResponse<PartitionKeyRange> partitionKeyRange = await client.ReadPartitionKeyRangeFeedAsync(
                                                                        collectionUri,
                                                                        new FeedOptions
                                                                            {
                                                                                RequestContinuation =
                                                                                    pkRangesResponseContinuation
                                                                            });
                partitionKeyRanges.AddRange(partitionKeyRange);
                pkRangesResponseContinuation = partitionKeyRange.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);

            foreach (var pkRange in partitionKeyRanges)
            {
                checkpoints.TryGetValue(pkRange.Id, out string continuation);
                var changeFeedQuery = client.CreateDocumentChangeFeedQuery(
                    collectionUri,
                    new ChangeFeedOptions
                        {
                            StartFromBeginning = true,
                            PartitionKeyRangeId = pkRange.Id,
                            RequestContinuation = continuation,
                            MaxItemCount = -1
                        });
                while (changeFeedQuery.HasMoreResults)
                {
                    var response = await changeFeedQuery.ExecuteNextAsync<MyDoc>();
                    foreach (var doc in response)
                    {
                        Console.WriteLine(
                            $"Change for product {doc.ProductId} and revision vector {doc.RevisionVector}");
                    }

                    checkpoints[pkRange.Id] = response.ResponseContinuation;
                }
            }

            return checkpoints;
        }
    }

    [JsonObject(NamingStrategyType = typeof(SnakeCaseNamingStrategy))]
    public class MyDoc
    {
        public string ProductId { get; set; }
        public string RevisionVector { get; set; }
    }
}
