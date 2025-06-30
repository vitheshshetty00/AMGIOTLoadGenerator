using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AMGIOTLoadGenerator.Utils
{
    public class CollectionScheduler
    {
        private readonly Dictionary<string, int> _frequencies;
        private readonly Dictionary<string, DateTime> _lastExecuted;
        private readonly Dictionary<string, Timer> _timers;
        private readonly Dictionary<string, Func<string, Task>> _actions;

        public CollectionScheduler(Dictionary<string, int> frequencies)
        {
            _frequencies = frequencies;
            _lastExecuted = new Dictionary<string, DateTime>();
            _timers = new Dictionary<string, Timer>();
            _actions = new Dictionary<string, Func<string, Task>>();

            foreach (var kvp in frequencies)
            {
                _lastExecuted[kvp.Key] = DateTime.MinValue;
            }
        }

        public void RegisterAction(string collectionName, Func<string, Task> action)
        {
            _actions[collectionName] = action;
        }

        public void Start()
        {
            foreach (var kvp in _frequencies)
            {
                var collectionName = kvp.Key;
                var frequencySeconds = kvp.Value;

                _timers[collectionName] = new Timer(
                    async _ => await ExecuteCollection(collectionName),
                    null,
                    TimeSpan.Zero, // Start immediately
                    TimeSpan.FromSeconds(frequencySeconds)
                );
            }
        }

        public void Stop()
        {
            foreach (var timer in _timers.Values)
            {
                timer?.Dispose();
            }
            _timers.Clear();
        }

        private async Task ExecuteCollection(string collectionName)
        {
            if (_actions.TryGetValue(collectionName, out var action))
            {
                try
                {
                    await action(collectionName);
                    _lastExecuted[collectionName] = DateTime.UtcNow;
                    
                }
                catch (Exception ex)
                {
                    Logger.Error($"Error executing collection {collectionName}: {ex.Message}");
                }
            }
        }

        public Dictionary<string, DateTime> GetLastExecutedTimes()
        {
            return new Dictionary<string, DateTime>(_lastExecuted);
        }
    }
}