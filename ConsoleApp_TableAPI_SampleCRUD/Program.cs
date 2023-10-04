using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using System.Collections.Concurrent;

//ref: https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/tables/Azure.Data.Tables/MigrationGuide.md

// Construct a new "TableServiceClient using a TableSharedKeyCredential.
String TableEndpoint = "https://....table.cosmos.azure.com:443/";
String TableAccontName = "...";
String TableAccontKey = "...";
String TableName = "...";

try
{
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, TableServiceClient Initialing ...");
    var serviceClient = new TableServiceClient(
        new Uri(TableEndpoint),
        new TableSharedKeyCredential(TableAccontName, TableAccontKey));

    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Connecting to {TableEndpoint} ...");
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Creating table [{TableName}] if not exists ...");

    // Create a new table. The TableItem class stores properties of the created table.
    TableItem table = serviceClient.CreateTableIfNotExists(TableName);
    var tableClient = serviceClient.GetTableClient(TableName);
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, The created table's name is {table.Name}.");


    String partitionKey = String.Format($"demo_{DateTime.UtcNow.ToString("yyyyMMdd_HHmmss")}");
    int RowKey = 1;
    var tableEntity = new TableEntity(partitionKey, RowKey.ToString());
    var random = new Random();
    int sleepTime = 1000;

    bool TestConfliict409 = false;
    while (true)
    {
        if (TestConfliict409 == true)
        {
            tableEntity.PartitionKey = "demo_conflict";
            tableEntity.RowKey = "rowkey_conflit";
        }
        else
        {
            tableEntity.PartitionKey = partitionKey;
            tableEntity.RowKey = RowKey.ToString();
        }        
        tableEntity.Add("entity_id", Guid.NewGuid().ToString());
        tableEntity.Add("entity_vale", 0);
        tableEntity.Add("entity_timestamp", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff"));

        //Insert
        var resultCreate = tableClient.AddEntityAsync(tableEntity);
        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} created ...");
        Thread.Sleep(sleepTime);

        //Update
        tableEntity["entity_vale"] = random.Next(1, 999);
        var resultUpsert = tableClient.UpsertEntityAsync(tableEntity);
        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} updated ...");
        Thread.Sleep(sleepTime);

        //Query
        Pageable<TableEntity> queryResultsFilter = tableClient.Query<TableEntity>(filter: $"PartitionKey eq '{partitionKey}' and RowKey eq '{RowKey}'");
        foreach (TableEntity qEntity in queryResultsFilter)
        {
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"" +
                $", \"entity_id\":\"{qEntity.GetString("entity_id")}" +
                $", \"entity_vale\":\"{qEntity.GetInt32("entity_vale")}\"" +
                $", \"entity_timestamp\":\"{qEntity.GetString("entity_timestamp")}\"}} retrieved ...");
        }

        //Delete
        var resultDelete = tableClient.DeleteEntityAsync(tableEntity.PartitionKey, tableEntity.RowKey);
        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} deleted ...");
        Thread.Sleep(sleepTime);
        
        RowKey++;
    }
}
catch (Exception ce)
{
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Error: {ce.Message.ToString()}");
}