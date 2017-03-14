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
        private const int maxConsumerCount = 10;

        public static void Main(string[] args)
        {
            Log.Enabled = false;

            for (int i = minConsumerCount; i <= maxConsumerCount; i++)
            {
                Run(i);
            }
        }

        private static void Run(int consumerCount)
        {
            var sw = new Stopwatch();
            sw.Start();

            var queue =
                new BufferBlock<EventDto>(new ExecutionDataflowBlockOptions
                {
//                    MaxDegreeOfParallelism = 1,
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

            var p1 = new Producer(typeMap);
            var p2 = new Producer(typeMap);
            var p3 = new Producer(typeMap);

            await Task.WhenAll(
                p1.Start(block, 2017, 1), 
                p2.Start(block, 2017, 2), 
                p3.Start(block, 2017, 3));

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

            var sql = $"SELECT E.[Event],T.[TypeName] AS [EventType] FROM [Events_{partition}] E JOIN [Types] T ON T.TypeId=E.TypeId";

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
        public async Task<List<object>> AwaitEvents(IReceivableSourceBlock<EventDto> source)
        {
            var events = new List<object>();

            while (await source.OutputAvailableAsync())
            {
                EventDto @event;

                if (source.TryReceive(null, out @event))
                {
                    events.Add(Serializer.NonGeneric.Deserialize(@event.EventType, @event.EventStream));
                }

                Log.Message($"{Thread.CurrentThread.ManagedThreadId} Consumer");
            }

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
