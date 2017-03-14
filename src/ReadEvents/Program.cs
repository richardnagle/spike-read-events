using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Fri.Xhl.Domain.Events;
using ProtoBuf;

namespace ReadEvents
{
    public class Program
    {
        private const int minConsumerCount = 1;
        private const int maxConsumerCount = 12;

        private const int producerCount = 12; // max 12

        public static void Main(string[] args)
        {
            Log.Enabled = false;

            for (int consumers = minConsumerCount; consumers <= maxConsumerCount; consumers++)
            {
                Run(consumers);
            }
        }

        private static void Run(int consumerCount)
        {
            var sw = new Stopwatch();
            sw.Start();

            var queue =
                new BufferBlock<EventDto>(new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Math.Min(maxConsumerCount, Environment.ProcessorCount)
//                    MaxMessagesPerTask = 50,
//                    BoundedCapacity  = 100 * consumerCount
                });

            var consumers = new List<Task<List<object>>>(consumerCount);

            for (var i = 0; i < consumerCount; i++)
            {
                consumers.Add(new Consumer().AwaitEvents(queue));
            }

            StartProducers(queue).Wait();

            var list = consumers.SelectMany(x => x.Result).ToList();

            sw.Stop();

            Console.WriteLine($"{consumerCount} consumers -> {list.Count} events read in {sw.ElapsedMilliseconds}ms");
        }

        private static async Task StartProducers(ITargetBlock<EventDto> block)
        {
            var typeMap = new TypeMap();

            var tasks = new Task[producerCount];
            
            for (int month = 1; month <= producerCount; month++)
            {
                tasks[month-1] = new Producer(typeMap).Start(block, 2016, month);
            }

            await Task.WhenAll(tasks);

            block.Complete();
        }
    }

    public static class Log
    {
        public static bool Enabled { get; set; }

        public static void Message(string message)
        {
            if (!Enabled) return;
            Console.WriteLine(message);
        }
    }

    public class EventDto: IDisposable
    {
        public Type EventType { get; set; }
        public Stream EventStream { get; set; }

        public void Dispose()
        {
            EventStream?.Dispose();
        }
    }

    public class Producer
    {
        private readonly TypeMap _typeMap;

        public Producer(TypeMap typeMap)
        {
            _typeMap = typeMap;
        }

        public async Task Start(ITargetBlock<EventDto> target, int yyyy, int mm)
        {
            Log.Message($"Start read for {mm}-{yyyy}");

            var partition = $"{yyyy}_{mm:D2}_01_00_00";

            var connectionString = "Data Source=Lon-DevSQL-2K8;Initial Catalog=EventStore_Trunk;Integrated Security=True";

            var sql = $"SELECT TOP 50000 E.[Event],T.[TypeName] AS [EventType] FROM [Events_{partition}] E JOIN [Types] T ON T.TypeId=E.TypeId";

            using (var cx = new SqlConnection(connectionString))
            {
                await cx.OpenAsync();

                using (var cmd = new SqlCommand(sql, cx))
                using (var rdr = await cmd.ExecuteReaderAsync())
                {
                    while (await rdr.ReadAsync())
                    {
                        await target.SendAsync(
                            new EventDto
                            {
                                EventType = _typeMap[rdr.GetString(1)],
                                EventStream = rdr.GetStream(0)
                            });
                        Log.Message($"{Thread.CurrentThread.ManagedThreadId} Producer");
                    }
                }
            }

            Log.Message($"Complete read for {mm}-{yyyy}");
        }
    }

    public class Consumer
    {
        private volatile int _count = 0;

        public async Task<List<object>> AwaitEvents(IReceivableSourceBlock<EventDto> source)
        {
            var events = new List<object>();

            while (await source.OutputAvailableAsync())
            {
                EventDto @event;

                if (source.TryReceive(null, out @event))
                {
                    events.Add(Serializer.NonGeneric.Deserialize(@event.EventType, @event.EventStream));
                    _count++;
                }

                Log.Message($"{Thread.CurrentThread.ManagedThreadId} Consumer");
            }

            Log.Message($"Consumer {Thread.CurrentThread.ManagedThreadId}: {_count} events processed");

            return events;
        }
    }

    public class TypeMap
    {
        private readonly Dictionary<string, Type> _map;

        public TypeMap()
        {
            var eventAssembly = typeof(IDomainEvent).Assembly;

            _map = eventAssembly
                .GetTypes()
                .Where(x => x.GetInterfaces().Contains(typeof(IDomainEvent)))
                .Where(x => !x.IsAbstract)
                .Where(x => x.IsClass)
                .ToDictionary(
                    key => key.Name,
                    value => value
                );
        }

        public Type this[string typeKey] => _map[typeKey];
    }
}
