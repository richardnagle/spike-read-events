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
using NLog;
using NLog.Config;
using NLog.Targets;
using ProtoBuf;

namespace ReadEvents
{
    public static class Constants
    {
        public static int MinConsumerCount = 2;
        public static int MaxConsumerCount = 2;

        public static int ConsumerSkip = 1;
        public static int MinProducerCount = 2;
        public static int MaxProducerCount = 2; // max 12
        public static int EventsToRead = 20000; // max 200,000. 0 for all

        public static readonly LogLevel LoggingLevel = LogLevel.Warn;
    }

    public class Program
    {
        private static Logger _logger;

        public static void Main(string[] args)
        {
            ConfigureLogging();
            _logger = LogManager.GetCurrentClassLogger();
            _logger.Info("started");

            SetFromCli(args, 0, x => Constants.MinConsumerCount = x);
            SetFromCli(args, 1, x => Constants.MaxConsumerCount = x);
            SetFromCli(args, 2, x => Constants.ConsumerSkip = x);
            SetFromCli(args, 3, x => Constants.MinProducerCount = x);
            SetFromCli(args, 4, x => Constants.MaxProducerCount = x);
            SetFromCli(args, 5, x => Constants.EventsToRead = x);

            var bus = MassTransit.Bus.Factory.CreateUsingRabbitMq(x =>
            {
                x.Host(new Uri("rabbitmq://localhost/mass-non-durable"), h =>
                {
                    h.Username("admin");
                    h.Password("admin");
                });

                x.Durable = false;

            });

            _logger.Info("starting bus");
            bus.Start();
            _logger.Info("started bus");

            for (int consumers = Constants.MinConsumerCount; consumers <= Constants.MaxConsumerCount; consumers+=Constants.ConsumerSkip)
            {
                for (int producers = Constants.MinProducerCount; producers <= Constants.MaxProducerCount; producers++)
                {
                    _logger.Info($"started run for {consumers} consumers, {producers} producers");
                    Run(producers, consumers, bus);
                    _logger.Info($"finished run for {consumers} consumers, {producers} producers");
                }
            }

            _logger.Info("stopping bus");
            bus.Stop();
            _logger.Info("stopped bus");

            _logger.Info("finished");
        }

        private static void SetFromCli(string[] args, int index, Action<int> action)
        {
            if (index >= args.Length) return;

            int value;

            if (!int.TryParse(args[index], out value)) return;

            action(value);
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

            _logger.Warn($"{producerCount} producers {consumerCount} consumers -> {eventCount:###,###,###} events read in {sw.ElapsedMilliseconds}ms");
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

        private static void ConfigureLogging()
        {
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget
            {
                Layout = @"${date:format=HH\:mm\:ss} [${pad:padding=5:inner=${level:uppercase=true}}] ${message}"
            };

            config.AddTarget("console", consoleTarget);
            config.AddRule(Constants.LoggingLevel, LogLevel.Fatal, consoleTarget);

            LogManager.Configuration = config;
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
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();

        private readonly TypeMap _typeMap;

        public Producer(TypeMap typeMap)
        {
            _typeMap = typeMap;
        }

        public async Task Start(ITargetBlock<EventDto> target, int yyyy, int mm, int producerCount)
        {
            _logger.Debug("Start read for {0}-{1}",mm,yyyy);

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
                        _logger.Trace("Producer {0}-{1} read on thread {2}",mm,yyyy,Thread.CurrentThread.ManagedThreadId);
                    }
                }
            }

            _logger.Debug("Complete read for {0}-{1}",mm,yyyy);
        }
    }

    public class Consumer
    {
        private readonly IBus _bus;

        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();

        private static int _instanceIdCounter = 1;
        private readonly int _instanceId = _instanceIdCounter++;

        public Consumer(IBus bus)
        {
            _bus = bus;
        }

        public async Task<int> AwaitEvents(IReceivableSourceBlock<EventDto> source)
        {
            var count = 0;

            _logger.Debug("Consumer {0} started", _instanceId);

            while (await source.OutputAvailableAsync())
            {
                EventDto eventDto;

                if (source.TryReceive(null, out eventDto))
                {
                    if (count==0) _logger.Info("Consumer {0} started consuming", _instanceId);
                    var @event = Serializer.NonGeneric.Deserialize(eventDto.EventType, eventDto.EventStream);
                    await _bus.Publish(@event);
                    count++;
                }

                _logger.Trace("Consumer {0} on thread {1}", _instanceId, Thread.CurrentThread.ManagedThreadId);
            }

            _logger.Debug("Consumer {0}: {1} events processed", _instanceId, count);

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
