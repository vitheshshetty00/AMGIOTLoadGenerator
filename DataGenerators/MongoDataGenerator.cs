using System.Globalization;
using CsvHelper;
using MongoDB.Bson;
using AMGIOTLoadGenerator.Utils;

namespace AMGIOTLoadGenerator.DataGenerators
{
    public class MongoDataGenerator : IMongoDataGenerator
    {
        private readonly Dictionary<string, List<BsonDocument>> _templateData;

        public MongoDataGenerator()
        {
            _templateData = new Dictionary<string, List<BsonDocument>>();
            LoadTemplateData();
        }

        private void LoadTemplateData()
        {
            var dataPath = "Data";

            var csvFiles = new[]
            {
                "OperatorMessagesHistory.csv",
                "OperationHistoryTransaction.csv",
                "AlarmLiveTransaction.csv",
                "AlarmHistoryTransaction.csv",
                "AlarmHistoryTemp.csv",
                "ProcessParameterTransaction.csv"
            };

            foreach (var csvFile in csvFiles)
            {
                var filePath = Path.Combine(dataPath, csvFile);
                if (File.Exists(filePath))
                {
                    var collectionName = Path.GetFileNameWithoutExtension(csvFile);
                    _templateData[collectionName] = LoadCsvDirectly(filePath);
                }
            }
        }

        private List<BsonDocument> LoadCsvDirectly(string filePath)
        {
            var docs = new List<BsonDocument>();
            
            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            
            // Read headers
            csv.Read();
            csv.ReadHeader();
            var headers = csv.HeaderRecord;
            
            if (headers == null || headers.Length == 0)
            {
                return docs;
            }

            // Read data rows
            while (csv.Read())
            {
                var doc = new BsonDocument();
                
                foreach (var header in headers)
                {
                    var value = csv.GetField(header);
                    doc[header] = ConvertToBsonValue(value);
                }
                
                docs.Add(doc);
            }
            
            return docs;
        }

        private static BsonValue ConvertToBsonValue(string value)
        {
            // Handle null/empty values
            if (string.IsNullOrEmpty(value) || 
                value.Equals("NULL", StringComparison.OrdinalIgnoreCase) ||
                value.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                return BsonNull.Value;
            }

            // Try integer (most common numeric type)
            if (int.TryParse(value, out int intValue))
                return new BsonInt32(intValue);

            // Try long for larger integers
            if (long.TryParse(value, out long longValue))
                return new BsonInt64(longValue);

            // Try double (covers decimal numbers)
            if (double.TryParse(value, out double doubleValue))
                return new BsonDouble(doubleValue);

            // Try DateTime with multiple formats
            if (DateTime.TryParse(value, out DateTime dateValue))
                return new BsonDateTime(DateTime.Now);

            // Try boolean
            if (bool.TryParse(value, out bool boolValue))
                return new BsonBoolean(boolValue);

            // Default to string
            return new BsonString(value);
        }

        public object GenerateData(string collectionName, string machineId)
        {
            if (!_templateData.ContainsKey(collectionName))
            {
                return new List<BsonDocument>();
            }

            var templateData = _templateData[collectionName];
            var result = new List<BsonDocument>(templateData.Count);


            for (int i = 0; i < templateData.Count; i++)
            {
                var template = templateData[i];

                // Ensure the document is a BsonDocument
                if (!(template is BsonDocument))
                {
                    throw new InvalidOperationException($"Template data for {collectionName} is not a BsonDocument.");
                }
            
                
                var newDoc = new BsonDocument(template);
                
                // Update machine-specific fields
                newDoc["MachineID"] = machineId;
                if (newDoc.Contains("AlarmTime"))
                {
                    newDoc["AlarmTime"] = DateTime.Now;
                }
                if (newDoc.Contains("TimeStamp"))
                {
                    newDoc["TimeStamp"] = DateTime.Now; 
                }
                if (newDoc.Contains("CycleStartTS"))
                {
                    newDoc["CycleStartTS"] = DateTime.Now + TimeSpan.FromSeconds(i * 3);
                    newDoc["CycleEndTS"] = DateTime.Now + TimeSpan.FromSeconds(i * 3 + 3); 
                }

                result.Add(newDoc);
            }

            return result;
        }
    }
}