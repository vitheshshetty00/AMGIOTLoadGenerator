using System;
using System.Collections.Generic;
using System.Data;
using AMGIOTLoadGenerator.Models;
using AMGIOTLoadGenerator.Utils;

namespace AMGIOTLoadGenerator.DataGenerators
{
    public class SqlDataGenerator : ISqlDataGenerator
    {
        private readonly Dictionary<string, DataTable> _templateData;
        private static readonly object _timestampLock = new object();
        private static long _timestampCounter = 0;

        public SqlDataGenerator()
        {
            _templateData = new Dictionary<string, DataTable>();
            LoadTemplateData();
        }

        private void LoadTemplateData()
        {
            // Focas_LiveData template
            _templateData["Focas_LiveData"] = CreateFocasLiveDataTemplate();

            // Focas_PredictiveMaintenance template
            _templateData["Focas_PredictiveMaintenance"] = CreateFocasPredictiveMaintenanceTemplate();

            // MachineStatusHistory template
            _templateData["MachineStatusHistory"] = CreateMachineStatusHistoryTemplate();

            // RawData template
            _templateData["RawData"] = CreateRawDataTemplate();

            // tcs_energyconsumption template
            _templateData["tcs_energyconsumption"] = CreateEnergyConsumptionTemplate();
        }

        public DataTable GenerateData(string tableName, MachineConfig machine)
        {
            if (!_templateData.ContainsKey(tableName))
            {
                return new DataTable(tableName);
            }

            var template = _templateData[tableName];
            
            // Use Copy() to get both structure AND data
            var result = template.Copy();
            
            // Generate unique base timestamp once
            DateTime baseTime;
            long counter;
            lock (_timestampLock)
            {
                counter = ++_timestampCounter;
                baseTime = DateTime.Now.AddMilliseconds(counter);
            }

            // Update machine-specific fields in existing rows (no need to create new rows)
            for (int i = 0; i < result.Rows.Count; i++)
            {
                UpdateMachineFields(result.Rows[i], machine, baseTime, tableName, i);
            }

            return result;
        }

        private void UpdateMachineFields(DataRow row, MachineConfig machine, DateTime baseTime, string tableName, int rowIndex)
        {
            var table = row.Table;
            var hash = machine.MachineID.GetHashCode() & 0x7FFFFFFF; // Positive hash

            switch (tableName)
            {
                case "Focas_LiveData":
                    if (table.Columns.Contains("MachineID")) row["MachineID"] = machine.MachineID;
                    if (table.Columns.Contains("CompanyID")) row["CompanyID"] = machine.CompanyID;
                    if (table.Columns.Contains("CNCTimeStamp"))
                        row["CNCTimeStamp"] = baseTime.AddMilliseconds(rowIndex * 3000); // Unique milliseconds
                    if (table.Columns.Contains("BatchTS")) row["BatchTS"] = baseTime.AddSeconds(3);
                    if (table.Columns.Contains("MachineUpDownBatchTS")) row["MachineUpDownBatchTS"] = baseTime.AddHours(-1);
                    break;

                case "Focas_PredictiveMaintenance":
                    if (table.Columns.Contains("MachineId")) row["MachineId"] = machine.MachineID;
                    if (table.Columns.Contains("CompanyID")) row["CompanyID"] = machine.CompanyID;
                    if (table.Columns.Contains("TimeStamp"))
                        row["TimeStamp"] = baseTime.AddTicks(hash % 10000);
                    break;

                case "MachineStatusHistory":
                    if (table.Columns.Contains("MachineID")) row["MachineID"] = machine.MachineID;
                    if (table.Columns.Contains("CompanyID")) row["CompanyID"] = machine.CompanyID;
                    if (table.Columns.Contains("UpdatedTS"))
                        row["UpdatedTS"] = baseTime.AddTicks(hash % 10000);
                    break;

                case "tcs_energyconsumption":
                    if (table.Columns.Contains("MachineID")) row["MachineID"] = machine.MachineID;
                    if (table.Columns.Contains("gtime")) row["gtime"] = baseTime.AddMinutes(-1);
                    if (table.Columns.Contains("gtime1")) row["gtime1"] = baseTime;
                    break;
                case "RawData":
                    if (table.Columns.Contains("Mc")) row["Mc"] = machine.MachineID;
                    if (table.Columns.Contains("Sttime")) row["Sttime"] = baseTime.AddTicks(hash % 10000);
                    if (table.Columns.Contains("Ndtime")) row["Ndtime"] = baseTime.AddTicks(hash % 10000).AddSeconds(30);
                    break;
            }
        }

        private DataTable CreateFocasLiveDataTemplate()
        {
            var dt = new DataTable("Focas_LiveData");
            dt.Columns.Add("MachineID", typeof(string));
            dt.Columns.Add("MachineStatus", typeof(string));
            dt.Columns.Add("MachineMode", typeof(string));
            dt.Columns.Add("ProgramNo", typeof(string));
            dt.Columns.Add("ToolNo", typeof(int));
            dt.Columns.Add("OffsetNo", typeof(int));
            dt.Columns.Add("SpindleStatus", typeof(string));
            dt.Columns.Add("SpindleSpeed", typeof(int));
            dt.Columns.Add("SpindleLoad", typeof(decimal));
            dt.Columns.Add("Temperature", typeof(decimal));
            dt.Columns.Add("SpindleTarque", typeof(decimal));
            dt.Columns.Add("FeedRate", typeof(decimal));
            dt.Columns.Add("AlarmNo", typeof(int));
            dt.Columns.Add("PowerOnTime", typeof(int));
            dt.Columns.Add("OperatingTime", typeof(int));
            dt.Columns.Add("CutTime", typeof(int));
            dt.Columns.Add("ServoLoad_XYZ", typeof(string));
            dt.Columns.Add("AxisPosition", typeof(string));
            dt.Columns.Add("ProgramBlock", typeof(string));
            dt.Columns.Add("CNCTimeStamp", typeof(DateTime));
            dt.Columns.Add("PartsCount", typeof(int));
            dt.Columns.Add("BatchTS", typeof(DateTime));
            dt.Columns.Add("MachineUpDownStatus", typeof(int));
            dt.Columns.Add("MachineUpDownBatchTS", typeof(DateTime));
            dt.Columns.Add("CompanyID", typeof(string));
            dt.Columns.Add("SyncedStatus", typeof(int));
            dt.Columns.Add("LiveAlarmsNo", typeof(string));

            // Add template rows with varying data
            var spindleSpeeds = new[] { 737, 738, 739 };
            var spindleLoads = new decimal[] { 4.000m, 5.000m, 8.000m };
            var temperatures = new decimal[] { 42.000m, 43.000m };
            var feedRates = new decimal[] { 96.000m, 118.000m };
            var powerOnTimes = new[] { 100, 90, 80, 70, 60, 50, 40, 30, 20, 10 };

            for (int i = 0; i < 10; i++)
            {
                var row = dt.NewRow();
                row["MachineID"] = "MachineID-1"; // Will be updated per machine
                row["MachineStatus"] = "In Cycle";
                row["MachineMode"] = "MEM";
                row["ProgramNo"] = "O179";
                row["ToolNo"] = 101;
                row["OffsetNo"] = 1;
                row["SpindleStatus"] = "RUNNING";
                row["SpindleSpeed"] = spindleSpeeds[i % spindleSpeeds.Length];
                row["SpindleLoad"] = spindleLoads[i % spindleLoads.Length];
                row["Temperature"] = temperatures[i % temperatures.Length];
                row["SpindleTarque"] = 0.000m;
                row["FeedRate"] = feedRates[i % feedRates.Length];
                row["AlarmNo"] = -1;
                row["PowerOnTime"] = powerOnTimes[i];
                row["OperatingTime"] = powerOnTimes[i];
                row["CutTime"] = powerOnTimes[i] - (powerOnTimes[i] * 30 / 100);
                row["ServoLoad_XYZ"] = DBNull.Value;
                row["AxisPosition"] = DBNull.Value;
                row["ProgramBlock"] = "GOD BUSH RUFFING 2ND";
                row["CNCTimeStamp"] = DateTime.Now; // Will be updated
                row["PartsCount"] = 1;
                row["BatchTS"] = DateTime.Now; // Will be updated
                row["MachineUpDownStatus"] = 1;
                row["MachineUpDownBatchTS"] = DateTime.Now; // Will be updated
                row["CompanyID"] = "ACE"; // Will be updated
                row["SyncedStatus"] = 0;
                row["LiveAlarmsNo"] = DBNull.Value;
                dt.Rows.Add(row);
            }

            return dt;
        }

        private DataTable CreateFocasPredictiveMaintenanceTemplate()
        {
            var dt = new DataTable("Focas_PredictiveMaintenance");
            dt.Columns.Add("MachineId", typeof(string));
            dt.Columns.Add("AlarmNo", typeof(int));
            dt.Columns.Add("TargetValue", typeof(decimal));
            dt.Columns.Add("ActualValue", typeof(decimal));
            dt.Columns.Add("TimeStamp", typeof(DateTime));
            dt.Columns.Add("CompanyID", typeof(string));
            dt.Columns.Add("AlarmDesc", typeof(string));
            dt.Columns.Add("CountType", typeof(int));
            dt.Columns.Add("SyncedStatus", typeof(int));

            var alarmDescs = new[]
            {
                "Luboil flow at all points",
                "Spindle Belt tensn,coupling",
                "check Tool.Ejctn,clmpng frc",
                "All springs in ATC pocket"
            };

            var syncedStatuses = new[] { 0, 1, 0, 0 };

            for (int i = 0; i < 4; i++)
            {
                var row = dt.NewRow();
                row["MachineId"] = "MachineId-1"; // Will be updated
                row["AlarmNo"] = 0;
                row["TargetValue"] = 4416.00m;
                row["ActualValue"] = 1387.28m;
                row["TimeStamp"] = DateTime.Now; // Will be updated
                row["CompanyID"] = "AMIT"; // Will be updated
                row["AlarmDesc"] = alarmDescs[i];
                row["CountType"] = 1;
                row["SyncedStatus"] = syncedStatuses[i];
                dt.Rows.Add(row);
            }

            return dt;
        }

        private DataTable CreateMachineStatusHistoryTemplate()
        {
            var dt = new DataTable("MachineStatusHistory");
            dt.Columns.Add("MachineID", typeof(string));
            dt.Columns.Add("CompanyID", typeof(string));
            dt.Columns.Add("EventID", typeof(string));
            dt.Columns.Add("Remarks", typeof(string));
            dt.Columns.Add("UpdatedTS", typeof(DateTime));
            dt.Columns.Add("SyncedStatus", typeof(int));

            var events = new[]
            {
                ("DataArrival", "Last Data has been Arrived"),
                ("CloudConnectedStatus", "Service Successfully Running"),
                ("CloudConnectedStatus", "Service Successfully Running"),
                ("CloudConnectedStatus", "Service Successfully Running"),
                ("DataArrival", "Last Data has been Arrived"),
                ("CloudConnectedStatus", "Service Successfully Running"),
                ("CloudConnectedStatus", "Service Successfully Running"),
                ("DataArrival", "Last Data has been Arrived"),
                ("DataArrival", "Last Data has been Arrived"),
                ("CloudConnectedStatus", "Service Successfully Running")
            };

            for (int i = 0; i < events.Length; i++)
            {
                var row = dt.NewRow();
                row["MachineID"] = "MachineID-1"; // Will be updated
                row["CompanyID"] = "1"; // Will be updated
                row["EventID"] = events[i].Item1;
                row["Remarks"] = events[i].Item2;
                row["UpdatedTS"] = DateTime.Now; // Will be updated
                row["SyncedStatus"] = 0;
                dt.Rows.Add(row);
            }

            return dt;
        }

        private DataTable CreateRawDataTemplate()
        {
            var dt = new DataTable("RawData");
            dt.Columns.Add("DataType", typeof(int));
            dt.Columns.Add("IPAddress", typeof(string));
            dt.Columns.Add("Mc", typeof(string));
            dt.Columns.Add("Comp", typeof(string));
            dt.Columns.Add("Opn", typeof(string));
            dt.Columns.Add("Opr", typeof(string));
            dt.Columns.Add("SPLSTRING1", typeof(int));
            dt.Columns.Add("Sttime", typeof(DateTime));
            dt.Columns.Add("Ndtime", typeof(DateTime));
            dt.Columns.Add("SPLSTRING2", typeof(string));
            dt.Columns.Add("Status", typeof(int));
            dt.Columns.Add("WorkOrderNumber", typeof(string));
            dt.Columns.Add("SPLString3", typeof(string));
            dt.Columns.Add("SPLString4", typeof(string));
            dt.Columns.Add("SPLString5", typeof(string));
            dt.Columns.Add("SPLString6", typeof(string));
            dt.Columns.Add("SPLString7", typeof(string));
            dt.Columns.Add("SPLString8", typeof(string));
            dt.Columns.Add("SyncedStatus", typeof(int));

            // Add 2 rows as per sample data
            var row1 = dt.NewRow();
            row1["DataType"] = 1;
            row1["IPAddress"] = "1";
            row1["Mc"] = "1";
            row1["Comp"] = "6519";
            row1["Opn"] = "30";
            row1["Opr"] = "9449";
            row1["SPLSTRING1"] = 1;
            row1["Sttime"] = DateTime.Now; // Will be updated
            row1["Ndtime"] = DateTime.Now; // Will be updated
            row1["SPLSTRING2"] = DBNull.Value;
            row1["Status"] = 0;
            row1["WorkOrderNumber"] = "0";
            row1["SyncedStatus"] = 0;
            dt.Rows.Add(row1);

            var row2 = dt.NewRow();
            row2["DataType"] = 2;
            row2["IPAddress"] = "1";
            row2["Mc"] = "1";
            row2["Comp"] = "6519";
            row2["Opn"] = "30";
            row2["Opr"] = "9449";
            row2["SPLSTRING1"] = DBNull.Value;
            row2["Sttime"] = DateTime.Now; // Will be updated
            row2["Ndtime"] = DateTime.Now; // Will be updated
            row2["SPLSTRING2"] = "5";
            row2["Status"] = 0;
            row2["WorkOrderNumber"] = "0";
            row2["SyncedStatus"] = 0;
            dt.Rows.Add(row2);

            return dt;
        }

        private DataTable CreateEnergyConsumptionTemplate()
        {
            var dt = new DataTable("tcs_energyconsumption");
            dt.Columns.Add("MachineID", typeof(string));
            dt.Columns.Add("gtime", typeof(DateTime));
            dt.Columns.Add("ampere", typeof(int));
            dt.Columns.Add("watt", typeof(decimal));
            dt.Columns.Add("pf", typeof(decimal));
            dt.Columns.Add("idd", typeof(int));
            dt.Columns.Add("KWH", typeof(decimal));
            dt.Columns.Add("gtime1", typeof(DateTime));
            dt.Columns.Add("ampere1", typeof(string));
            dt.Columns.Add("KWH1", typeof(decimal));
            dt.Columns.Add("Volt1", typeof(int));
            dt.Columns.Add("Volt2", typeof(int));
            dt.Columns.Add("Volt3", typeof(int));
            dt.Columns.Add("AmpereR", typeof(decimal));
            dt.Columns.Add("AmpereY", typeof(decimal));
            dt.Columns.Add("AmpereB", typeof(decimal));
            dt.Columns.Add("KVA", typeof(int));
            dt.Columns.Add("EnergySource", typeof(int));
            dt.Columns.Add("CompanyIotID", typeof(int));
            dt.Columns.Add("Volt4", typeof(int));
            dt.Columns.Add("Volt5", typeof(int));
            dt.Columns.Add("Volt6", typeof(int));
            dt.Columns.Add("SyncedStatus", typeof(int));

            var row = dt.NewRow();
            row["MachineID"] = "1"; // Will be updated
            row["gtime"] = DateTime.Now; // Will be updated
            row["ampere"] = 0;
            row["watt"] = 0.90953m;
            row["pf"] = 0.63m;
            row["idd"] = 483195103;
            row["KWH"] = 66524.14844m;
            row["gtime1"] = DateTime.Now; // Will be updated
            row["ampere1"] = DBNull.Value;
            row["KWH1"] = 66524.17188m;
            row["Volt1"] = 239;
            row["Volt2"] = 240;
            row["Volt3"] = 241;
            row["AmpereR"] = 1.71883m;
            row["AmpereY"] = 1.30476m;
            row["AmpereB"] = 2.97324m;
            row["KVA"] = 0;
            row["EnergySource"] = 1;
            row["CompanyIotID"] = 1; // Will be updated
            row["Volt4"] = 415;
            row["Volt5"] = 417;
            row["Volt6"] = 416;
            row["SyncedStatus"] = 0;
            dt.Rows.Add(row);

            return dt;
        }
    }
}