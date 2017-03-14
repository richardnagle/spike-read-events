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
                var host = cfg.Host(new Uri("rabbitmq://localhost/"), h =>
                {
                    h.Username("guest");
                    h.Password("guest");
                });

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
        public async Task Consume(ConsumeContext<Message> context)
        {
            var message = context.Message;
            await Console.Out.WriteLineAsync($"{DateTime.Now.ToLongTimeString()} Received event {message.GetType()}");

        }
    }
}
