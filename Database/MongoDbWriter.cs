using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using AMGIOTLoadGenerator.Utils;
using AMGIOTLoadGenerator.Models;

namespace AMGIOTLoadGenerator.Database
{
    public class MongoDbWriter
    {
        private static readonly Lazy<ConfigRoot> _lazyConfig = new Lazy<ConfigRoot>(() =>
        {
            var configText = File.ReadAllText("Config/appsettings.json");
            return JsonSerializer.Deserialize<ConfigRoot>(configText);
        });

        private static readonly Lazy<IMongoClient> _lazyClient = new Lazy<IMongoClient>(() =>
        {
            var config = _lazyConfig.Value;
            var settings = MongoClientSettings.FromConnectionString(config.MongoDB.ConnectionString);
            settings.MaxConnectionPoolSize = 100;
            settings.MinConnectionPoolSize = 5;
            settings.WaitQueueTimeout = TimeSpan.FromSeconds(10);
            settings.ConnectTimeout = TimeSpan.FromSeconds(5);
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(5);
            return new MongoClient(settings);
        });

        private static readonly Lazy<IMongoDatabase> _lazyDatabase = new Lazy<IMongoDatabase>(() =>
        {
            var config = _lazyConfig.Value;
            return _lazyClient.Value.GetDatabase(config.MongoDB.Database);
        });

        private static IMongoDatabase Database => _lazyDatabase.Value;
        private SemaphoreSlim writeThrottle = new SemaphoreSlim(50);

        public async Task InsertDataAsync(string collectionName, List<BsonDocument> data)
        {
            if (data == null || data.Count == 0) return;

            await writeThrottle.WaitAsync();
            try
            {
                await Database.GetCollection<BsonDocument>(collectionName)
                        .InsertManyAsync(data, new InsertManyOptions { IsOrdered = false });
                await Task.Delay(50);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error inserting data into {collectionName}: {ex.Message}");
            }
            finally
            {
                writeThrottle.Release();
            }
        }

        public void TestConnection()
        {
            try
            {
                var collections = Database.ListCollectionNames().ToList();
                Logger.Info($"✓ MongoDB connection successful! Found {collections.Count} collections.");
            }
            catch (Exception ex)
            {
                Logger.Error($"MongoDB connection test failed: {ex.Message}");
                throw;
            }
        }
    }
}