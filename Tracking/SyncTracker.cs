using System.Collections.Generic;
using System.Collections.Concurrent;
using AMGIOTLoadGenerator.Models;

namespace AMGIOTLoadGenerator.Tracking
{
    public class SyncTracker
    {
        private readonly ConcurrentDictionary<string, TableInsertStatus> _statuses = new();

        public void TrackInsert(TableInsertStatus status)
        {
            _statuses.AddOrUpdate(status.Name, status, (key, existing) =>
            {
                existing.RecordsInserted += status.RecordsInserted;
                existing.RecordsSynced += status.RecordsSynced;
                existing.LastInsertTime = status.LastInsertTime;
                return existing;
            });
        }

        public IEnumerable<TableInsertStatus> GetStatuses() => _statuses.Values;

        public void Reset()
        {
            _statuses.Clear();
        }
    }
}