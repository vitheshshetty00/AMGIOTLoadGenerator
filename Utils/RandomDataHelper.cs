using System;
namespace AMGIOTLoadGenerator.Utils
{
    public static class RandomDataHelper
    {
        private static readonly Random _random = new();
        public static int NextInt(int min, int max) => _random.Next(min, max);
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var stringChars = new char[length];
            for (int i = 0; i < length; i++)
                stringChars[i] = chars[_random.Next(chars.Length)];
            return new string(stringChars);
        }
    }
}