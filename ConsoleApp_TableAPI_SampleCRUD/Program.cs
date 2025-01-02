using Azure.Identity;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;


//ref: https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/tables/Azure.Data.Tables/MigrationGuide.md
// Construct a new "TableServiceClient using a TableSharedKeyCredential.
String aad_tenant_id = "...";
String aad_managed_user_id = "..."; //User Managed Identity: etumi-001-cosmosapp
String TableEndpoint = "https://....table.cosmos.azure.com:443/";
String TableAccontName = "...";
String TableAccontKey = "...";
String TableName = "..."; 
TableServiceClient serviceClient = null;

try
{
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, TableServiceClient Initialing ...");

    //remove Key-Based Authentication
    /*
    serviceClient = new TableServiceClient(
        new Uri(TableEndpoint),
        new TableSharedKeyCredential(TableAccontName, TableAccontKey)
    );
    */

    String application_env = "local";
    if (application_env.ToLower() == "local")
    {
        //Locally, use DefaultAzureCredential
        var tokenCredential = new DefaultAzureCredential(
            new DefaultAzureCredentialOptions
            {
                TenantId = aad_tenant_id
            });
        serviceClient = new TableServiceClient(
            new Uri(TableEndpoint),
            tokenCredential
        );
    }
    else if (application_env.ToLower() == "cloud")
    {
        TableName = String.Format($"SampleTable{DateTime.UtcNow.ToString("yyyyMMdd-HHmmss")}");

        //Azure side, use ManagedIdentityCredential
        var tokenCredential = new ManagedIdentityCredential(
            clientId: aad_managed_user_id,
            new DefaultAzureCredentialOptions
            {
                TenantId = aad_tenant_id
            });
        serviceClient = new TableServiceClient(
            new Uri(TableEndpoint),
            tokenCredential
        );
    }

    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Connecting to {TableEndpoint} ...");
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Creating table [{TableName}] if not exists ...");

    // Create a new table. The TableItem class stores properties of the created table.
    TableItem table = serviceClient.CreateTableIfNotExists(TableName);
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, The created table's name is {table.Name}.");
    var tableClient = serviceClient.GetTableClient(TableName);


    String partitionKey = String.Format($"demo_{DateTime.UtcNow.ToString("yyyyMMdd_HHmmss")}");
    int RowKey = 1;
    var tableEntity = new TableEntity(partitionKey, RowKey.ToString());
    var random = new Random();
    int sleepTime = 5000;

    bool ifInsert = true;
    bool ifUpdate = true;
    bool ifQuery = true;
    bool ifDelete = true;

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
        if(ifInsert)
        {
            var resultCreate = tableClient.AddEntityAsync(tableEntity);
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} created ...");
            Thread.Sleep(sleepTime);
        }

        //Update
        if (ifUpdate)
        {
            tableEntity["entity_vale"] = random.Next(1, 999);
            var resultUpsert = tableClient.UpsertEntityAsync(tableEntity);
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} updated ...");
            Thread.Sleep(sleepTime);
        }

        //Query
        if (ifQuery)
        {
            Pageable<TableEntity> queryResultsFilter = tableClient.Query<TableEntity>(filter: $"PartitionKey eq '{partitionKey}' and RowKey eq '{RowKey}'");
            foreach (TableEntity qEntity in queryResultsFilter)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"" +
                    $", \"entity_id\":\"{qEntity.GetString("entity_id")}" +
                    $", \"entity_vale\":\"{qEntity.GetInt32("entity_vale")}\"" +
                    $", \"entity_timestamp\":\"{qEntity.GetString("entity_timestamp")}\"}} retrieved ...");
            }
        }

        //Delete
        if (ifDelete)
        {
            var resultDelete = tableClient.DeleteEntityAsync(tableEntity.PartitionKey, tableEntity.RowKey);
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, entity {{\"PartitionKey\":\"{tableEntity.PartitionKey}\", \"RowKey\":\"{tableEntity.RowKey}\"}} deleted ...");
            Thread.Sleep(sleepTime);
        }

        RowKey++;
    }
}
catch (Exception ce)
{
    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Error: {ce.Message.ToString()}");
}