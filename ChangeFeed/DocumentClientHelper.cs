// Copyright (c) Microsoft Corporation. All rights reserved.

namespace ChangeFeed
{
    using System;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    public static class DocumentClientHelper
    {
        private static readonly Uri ServiceEndpoint = new Uri(ConfigurationManager.AppSettings["endpoint"]);
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["authKey"];

        private static readonly Lazy<IDocumentClient> Client =
            new Lazy<IDocumentClient>(() => new DocumentClient(ServiceEndpoint, PrimaryKey));

        public static async Task CreateCollection(string collection)
        {
            var result = await Client.Value.CreateDocumentCollectionAsync(
                             UriFactory.CreateDatabaseUri(DocumentCollections.DbName),
                             new DocumentCollection
                                 {
                                     Id = collection,
                                     DefaultTimeToLive = -1
                                 });
        }

        public static async Task AddItem(string collection, string itemName)
        {
            await Client.Value.CreateDocumentAsync(GetCollectionUri(collection), new ToDoItem
                {
                    Name = itemName,
                    TimeToLive = -1
                });
        }

        public static async Task DeleteItem(string collection, string itemName)
        {
            var item = await GetItem(collection, itemName);
            item.TimeToLive = 1; // doc expires after 1 second
            await Client.Value.UpsertDocumentAsync(GetCollectionUri(collection), item);
        }

        public static async Task<ToDoItem> GetItem(string collection, string itemName)
        {
            IDocumentQuery<ToDoItem> query = Client
                                             .Value.CreateDocumentQuery<ToDoItem>(
                                                 GetCollectionUri(collection))
                                             .Where(x => x.Name == itemName)
                                             .AsDocumentQuery();
            var result = await query.ExecuteNextAsync<ToDoItem>();
            return result.SingleOrDefault();
        }

        private static Uri GetCollectionUri(string collection)
        {
            return UriFactory.CreateDocumentCollectionUri(DocumentCollections.DbName, collection);
        }
    }
}

    
