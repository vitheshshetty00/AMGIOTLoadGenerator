using System;
using System.IO;

namespace AMGIOTLoadGenerator.Utils
{
    public static class Logger
    {
        private static readonly object _lock = new();
        private static readonly string LogDir = "Logs";
        private static readonly string LogFile = Path.Combine(LogDir, $"log_{DateTime.UtcNow:yyyyMMdd}.txt");

        static Logger()
        {
            if (!Directory.Exists(LogDir))
                Directory.CreateDirectory(LogDir);
        }

        public static void Info(string message) => Log("INFO", message, ConsoleColor.Cyan);
        public static void Warn(string message) => Log("WARN", message, ConsoleColor.Yellow);
        public static void Error(string message) => Log("ERROR", message, ConsoleColor.Red);

        private static void Log(string level, string message, ConsoleColor color)
        {
            var logEntry = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] [{level}] {message}";
            lock (_lock)
            {
                File.AppendAllText(LogFile, logEntry + Environment.NewLine);
                var prevColor = Console.ForegroundColor;
                Console.ForegroundColor = color;
                Console.WriteLine(logEntry);
                Console.ForegroundColor = prevColor;
            }
        }
    }
}