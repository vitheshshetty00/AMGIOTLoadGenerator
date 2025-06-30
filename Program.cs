using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using AMGIOTLoadGenerator.DataGenerators;
using AMGIOTLoadGenerator.Database;
using AMGIOTLoadGenerator.Models;
using AMGIOTLoadGenerator.Tracking;
using System.Diagnostics;
using AMGIOTLoadGenerator.Utils;
using System.Linq;
using System.Data;
using MongoDB.Bson;

class Program
{
    private static CollectionScheduler _mongoScheduler;
    private static List<MachineConfig> _machines;
    private static MongoDataGenerator _mongoGen;
    private static MongoDbWriter _mongoWriter;
    private static SyncTracker _tracker;

    static async Task Main(string[] args)
    {
        // Styled header
        Console.Clear();
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("==============================");
        Console.WriteLine("   AMGIOT Load Simulator v2.0  ");
        Console.WriteLine("==============================");
        Console.ResetColor();
        Logger.Info("Starting load simulation...");

        // Load config
        var configText = await File.ReadAllTextAsync("Config/appsettings.json");
        var config = JsonSerializer.Deserialize<ConfigRoot>(configText);

        // Initialize components
        var sqlGen = new SqlDataGenerator();
        _mongoGen = new MongoDataGenerator();
        var sqlWriter = new SqlDbWriter();
        _mongoWriter = new MongoDbWriter();
        _tracker = new SyncTracker();

        _machines = new List<MachineConfig>();
        for (int i = 1; i <= config.MachineCount; i++)
        {
            _machines.Add(new MachineConfig
            {
                MachineID = $"M-{i:D3}",
                PlantID = "Plant-01",
                CompanyID = "Ace"
            });
        }

        // Pre-warm connections
        Logger.Info("Pre-warming database connections...");
        sqlWriter.TestConnection();
        _mongoWriter.TestConnection();

        // Initialize MongoDB scheduler with individual frequencies
        _mongoScheduler = new CollectionScheduler(config.MongoCollectionFrequencies);

        // Register MongoDB collection actions
        foreach (var collection in config.CollectionsToSync)
        {
            _mongoScheduler.RegisterAction(collection, ProcessMongoCollection);
        }

        // Start MongoDB scheduler
        _mongoScheduler.Start();
        Logger.Info("MongoDB collection scheduler started with individual frequencies");

        int cycle = 0;
        while (true)
        {
            cycle++;
            Logger.Info($"\n--- SQL Cycle {cycle} ---");
            var sw = Stopwatch.StartNew();

            // Process only SQL data in the main cycle
            await ProcessSqlData(_machines, config, sqlGen, sqlWriter);

            sw.Stop();

            // Display results
            DisplayResults(cycle, sw, config);

            await Task.Delay(config.CycleDurationSeconds * 1000);
        }
    }

    private static async Task ProcessMongoCollection(string collectionName)
    {
        try
        {
            var mongoBatch = new List<BsonDocument>();

            // Generate data sequentially for all machines for this collection
            foreach (var machine in _machines)
            {
                var data = (List<BsonDocument>)_mongoGen.GenerateData(collectionName, machine.MachineID);
                mongoBatch.AddRange(data);
                await _mongoWriter.InsertDataAsync(collectionName, data);
            }

            // Insert the batch
            if (mongoBatch.Count > 0)
            {
                

                _tracker.TrackInsert(new TableInsertStatus
                {
                    Name = collectionName,
                    MachineID = "ALL",
                    RecordsInserted = mongoBatch.Count,
                    RecordsSynced = 0,
                    LastInsertTime = DateTime.UtcNow
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"Error processing MongoDB collection {collectionName}: {ex.Message}");
        }
    }

    private static async Task ProcessSqlData(
        List<MachineConfig> machines,
        ConfigRoot config,
        SqlDataGenerator sqlGen,
        SqlDbWriter sqlWriter)
    {
        var sqlBatches = new Dictionary<string, List<DataTable>>();

        foreach (var table in config.TransactionTables)
            sqlBatches[table] = new List<DataTable>();

        // Generate SQL data in parallel
        var dataGenerationTasks = machines.AsParallel()
            .WithDegreeOfParallelism(Environment.ProcessorCount)
            .Select(machine =>
            {
                var sqlData = new Dictionary<string, DataTable>();

                foreach (var table in config.TransactionTables)
                {
                    sqlData[table] = sqlGen.GenerateData(table, machine);
                }

                return new { Machine = machine, SqlData = sqlData };
            })
            .ToList();

        // Batch the generated data
        foreach (var result in dataGenerationTasks)
        {
            foreach (var kvp in result.SqlData)
            {
                sqlBatches[kvp.Key].Add(kvp.Value);
            }
        }

        // SQL insertions
        var insertionTasks = new List<Task>();
        foreach (var kvp in sqlBatches)
        {
            if (kvp.Value.Count > 0)
            {
                insertionTasks.Add(Task.Run(async () =>
                {
                    var mergedTable = MergeDataTables(kvp.Value, kvp.Key);
                    await sqlWriter.InsertDataAsync(kvp.Key, mergedTable);
                    _tracker.TrackInsert(new TableInsertStatus
                    {
                        Name = kvp.Key,
                        MachineID = "ALL",
                        RecordsInserted = mergedTable.Rows.Count,
                        RecordsSynced = 0,
                        LastInsertTime = DateTime.UtcNow
                    });
                }));
            }
        }

        await Task.WhenAll(insertionTasks);
    }

    private static DataTable MergeDataTables(List<DataTable> tables, string tableName)
    {
        if (tables.Count == 0) return new DataTable(tableName);
        if (tables.Count == 1) return tables[0];

        var merged = tables[0].Copy();

        for (int i = 1; i < tables.Count; i++)
        {
            merged.Merge(tables[i], false, MissingSchemaAction.Ignore);
        }

        return merged;
    }

    private static void DisplayResults(int cycle, Stopwatch sw, ConfigRoot config)
    {
        var statuses = _tracker.GetStatuses();
        var sqlTotals = new Dictionary<string, int>();
        var mongoTotals = new Dictionary<string, int>();

        foreach (var status in statuses)
        {
            if (config.TransactionTables.Contains(status.Name))
            {
                sqlTotals[status.Name] = status.RecordsInserted;
            }
            else if (config.CollectionsToSync.Contains(status.Name))
            {
                mongoTotals[status.Name] = status.RecordsInserted;
            }
        }

        // Get last executed times for MongoDB collections
        var lastExecutedTimes = _mongoScheduler.GetLastExecutedTimes();

        var statusMsg = $"SQL Cycle: {cycle} | Elapsed: {sw.ElapsedMilliseconds} ms | Next in: {config.CycleDurationSeconds}s";
        var sqlMsg = $"SQL: {string.Join(" | ", sqlTotals.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}";
        var mongoMsg = $"MongoDB: {string.Join(" | ", mongoTotals.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}";
        var freqMsg = $"Frequencies: {string.Join(" | ", config.MongoCollectionFrequencies.Select(kvp => $"{kvp.Key}: {kvp.Value}s"))}";

        var origTop = Console.CursorTop;

        // Status lines
        Console.SetCursorPosition(0, Console.WindowTop + Console.WindowHeight - 5);
        Console.BackgroundColor = ConsoleColor.DarkBlue;
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(statusMsg.PadRight(Console.WindowWidth - 1));
        Console.ResetColor();

        Console.SetCursorPosition(0, Console.WindowTop + Console.WindowHeight - 4);
        Console.BackgroundColor = ConsoleColor.DarkGreen;
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(sqlMsg.PadRight(Console.WindowWidth - 1));
        Console.ResetColor();

        Console.SetCursorPosition(0, Console.WindowTop + Console.WindowHeight - 3);
        Console.BackgroundColor = ConsoleColor.DarkMagenta;
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(mongoMsg.PadRight(Console.WindowWidth - 1));
        Console.ResetColor();

        Console.SetCursorPosition(0, Console.WindowTop + Console.WindowHeight - 2);
        Console.BackgroundColor = ConsoleColor.DarkCyan;
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(freqMsg.PadRight(Console.WindowWidth - 1));
        Console.ResetColor();

        Console.SetCursorPosition(0, origTop);
    }
}

