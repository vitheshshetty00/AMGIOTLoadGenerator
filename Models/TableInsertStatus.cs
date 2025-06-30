using System;
namespace AMGIOTLoadGenerator.Models
{
    public class TableInsertStatus
    {
        public string Name { get; set; }
        public string MachineID { get; set; }
        public int RecordsInserted { get; set; }
        public int RecordsSynced { get; set; }
        public DateTime LastInsertTime { get; set; }
        public DateTime? LastSyncTime { get; set; }
    }
}