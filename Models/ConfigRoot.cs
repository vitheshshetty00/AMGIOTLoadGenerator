namespace AMGIOTLoadGenerator.Models;

public class ConfigRoot
{
    public int MachineCount { get; set; }
    public int CycleDurationSeconds { get; set; }
    public int DowntimeSeconds { get; set; }
    public string[] TransactionTables { get; set; }
    public string[] CollectionsToSync { get; set; }
    public Dictionary<string, int> MongoCollectionFrequencies { get; set; } = new Dictionary<string, int>();
    public SqlConfig Sql { get; set; }
    public MongoConfig MongoDB { get; set; }
}
public class SqlConfig { public required string ConnectionString { get; set; } }
public class MongoConfig { public required string ConnectionString { get; set; } public required string Database { get; set; } }

