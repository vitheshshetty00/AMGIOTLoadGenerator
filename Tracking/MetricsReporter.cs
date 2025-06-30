using System;
using System.Collections.Generic;
using AMGIOTLoadGenerator.Models;
namespace AMGIOTLoadGenerator.Tracking
{
    public class MetricsReporter
    {
        public void Report(IEnumerable<TableInsertStatus> statuses)
        {
            foreach (var status in statuses)
            {
                Console.WriteLine($"[{status.Name}] Machine: {status.MachineID}, Inserted: {status.RecordsInserted}, Synced: {status.RecordsSynced}, LastInsert: {status.LastInsertTime}, LastSync: {status.LastSyncTime}");
            }
        }
    }
}