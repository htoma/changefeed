// Copyright (c) Microsoft Corporation. All rights reserved.

namespace ChangeFeed
{
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;

    public class ToDoItem : Document
    {
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }
    }
}
