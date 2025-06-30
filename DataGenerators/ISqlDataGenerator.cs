using System.Data;
using AMGIOTLoadGenerator.Models;

namespace AMGIOTLoadGenerator.DataGenerators
{
    public interface ISqlDataGenerator
    {
        DataTable GenerateData(string tableName, MachineConfig machine);
    }
}