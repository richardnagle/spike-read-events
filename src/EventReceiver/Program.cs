using System;
using System.Threading.Tasks;
using Fri.Xhl.Domain.Events;
using MassTransit;

namespace EventReceiver
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var bus = MassTransit.Bus.Factory.CreateUsingRabbitMq(cfg =>
            {
                var host = cfg.Host(new Uri("rabbitmq://devwrkstn00419/"), h =>
                {
                    h.Username("admin");
                    h.Password("admin");
                });

                cfg.Durable = false;

                cfg.ReceiveEndpoint(host, "event_receiver_queue", e =>
                {
                    e.Consumer<EventConsumer>();
                });

            });

            bus.Start();
        }
    }

    public class EventConsumer: IConsumer<Message>
    {
        private static int count = 0;

        public async Task Consume(ConsumeContext<Message> context)
        {
            count++;

            if (count%1000 == 0)
            {
                await Console.Out.WriteLineAsync($"{count} messages received");
            }
        }
    }
}
