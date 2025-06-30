namespace AMGIOTLoadGenerator.DataGenerators
{
    public interface IMongoDataGenerator
    {
        object GenerateData(string collectionName, string machineId);
    }
}