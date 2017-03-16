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
using MassTransit;
using ProtoBuf;

namespace ReadEvents
{
    public static class Constants
    {
        public const int MinConsumerCount = 25;
        public const int MaxConsumerCount = 25;

        public const int MinProducerCount = 2;
        public const int MaxProducerCount = 2; // max 12

        public const int EventsToRead = 20000; // max 200,000. 0 for all
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Enabled = false;

            var bus = MassTransit.Bus.Factory.CreateUsingRabbitMq(x =>
            {
                x.Host(new Uri("rabbitmq://localhost/"), h =>
                {
                    h.Username("guest");
                    h.Password("guest");
                });

            });

            bus.Start();


            for (int producers = Constants.MinProducerCount; producers <= Constants.MaxProducerCount; producers++)
            {
                for (int consumers = Constants.MinConsumerCount; consumers <= Constants.MaxConsumerCount; consumers++)
                {
                    Run(producers, consumers, bus);
                }
            }

            Console.WriteLine("stopping bus");
            bus.Stop();
        }

        private static void Run(int producerCount, int consumerCount, IBus bus)
        {
            var sw = new Stopwatch();
            sw.Start();

            var queue =
                new BufferBlock<EventDto>(new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Math.Min(Constants.MaxConsumerCount, Environment.ProcessorCount)
//                    MaxMessagesPerTask = 50,
//                    BoundedCapacity  = 100 * consumerCount
                });



            var consumers = new List<Task<int>>(consumerCount);

            for (var i = 0; i < consumerCount; i++)
            {
                consumers.Add(new Consumer(bus).AwaitEvents(queue));
            }

            StartProducers(queue, producerCount).Wait();

            var eventCount = consumers.Sum(x => x.Result);

            sw.Stop();

            Console.WriteLine($"{producerCount} producers {consumerCount} consumers -> {eventCount:###,###,###} events read in {sw.ElapsedMilliseconds}ms");
        }

        private static async Task StartProducers(ITargetBlock<EventDto> block, int producerCount)
        {
            var typeMap = new TypeMap();

            var tasks = new Task[producerCount];
            
            for (int month = 1; month <= producerCount; month++)
            {
                tasks[month-1] = new Producer(typeMap).Start(block, 2016, month, producerCount);
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

        public async Task Start(ITargetBlock<EventDto> target, int yyyy, int mm, int producerCount)
        {
            Log.Message($"Start read for {mm}-{yyyy}");

            var partition = $"{yyyy}_{mm:D2}_01_00_00";

            var connectionString = "Data Source=Lon-DevSQL-2K8;Initial Catalog=EventStore_Trunk;Integrated Security=True";

            string top = "";
            if (Constants.EventsToRead > 0)
            {
                int recordCount = Constants.EventsToRead / producerCount;
                top = $"TOP {recordCount}";
            }

            var sql = $"SELECT {top} E.[Event],T.[TypeName] AS [EventType] FROM [Events_{partition}] E JOIN [Types] T ON T.TypeId=E.TypeId";

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
        private readonly IBus _bus;

        public Consumer(IBus bus)
        {
            _bus = bus;
        }

        public async Task<int> AwaitEvents(IReceivableSourceBlock<EventDto> source)
        {
            var count = 0;

            while (await source.OutputAvailableAsync())
            {
                EventDto eventDto;

                if (source.TryReceive(null, out eventDto))
                {
                    var @event = Serializer.NonGeneric.Deserialize(eventDto.EventType, eventDto.EventStream);
                    await _bus.Publish(@event);
                    count++;
                }

                Log.Message($"{Thread.CurrentThread.ManagedThreadId} Consumer");
            }

            Log.Message($"Consumer {Thread.CurrentThread.ManagedThreadId}: {count} events processed");

            return count;
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
