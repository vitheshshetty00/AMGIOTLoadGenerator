using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AMGIOTLoadGenerator.Utils;
using AMGIOTLoadGenerator.Models;

namespace AMGIOTLoadGenerator.Database
{
    public class SqlDbWriter
    {
        private static readonly Lazy<string> _lazyConnectionString = new Lazy<string>(() =>
        {
            var configText = File.ReadAllText("Config/appsettings.json");
            var config = JsonSerializer.Deserialize<ConfigRoot>(configText);
            return config.Sql.ConnectionString;
        });

        private static string ConnectionString => _lazyConnectionString.Value;
        private static readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(5, 5); // Limit concurrent connections

        public void InsertData(string tableName, DataTable data)
        {
            if (data == null || data.Rows.Count == 0) return;

            InsertDataAsync(tableName, data).Wait();
        }

        public async Task InsertDataAsync(string tableName, DataTable data)
        {
            if (data == null || data.Rows.Count == 0) return;

            await _connectionSemaphore.WaitAsync();
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                await connection.OpenAsync();

                using var bulkCopy = new SqlBulkCopy(connection)
                {
                    DestinationTableName = tableName,
                    BulkCopyTimeout = 300,
                    BatchSize = 50000, // Increased batch size
                    EnableStreaming = true,
                    NotifyAfter = 0 // Disable notifications for better performance
                };

                // Pre-map columns for better performance
                foreach (DataColumn column in data.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(data);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error bulk inserting data into {tableName}: {ex.Message}");
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        public void TestConnection()
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                connection.Open();
                Logger.Info("✓ SQL Server connection successful!");
                connection.Close();
            }
            catch (Exception ex)
            {
                Logger.Error($"✗ SQL Server connection test failed: {ex.Message}");
                throw;
            }
           
        }
    }
}