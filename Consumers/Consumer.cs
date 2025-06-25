using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Consumers
{
    public static class Consumer
    {
        public static async Task Register(IChannel channel, string exchangeName, string queueName, string routingKey)
        {
                await channel.BasicQosAsync(0, 10, false);
                await channel.QueueDeclareAsync(queueName, false, false, false, null);
                await channel.QueueBindAsync(queueName, exchangeName, routingKey, null);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (sender, e) =>
                {
                    //throw new Exception("Error has occured");
                    var body = e.Body;
                    var message = JsonSerializer.Deserialize<MessageDto>(Encoding.UTF8.GetString(body.ToArray()));
                    Console.WriteLine($"{DateTime.Now} Received message: {message.Content}");
                    Thread.Sleep(TimeSpan.FromSeconds(2)); // Имитация долгой обработки
                    await channel.BasicAckAsync(e.DeliveryTag, false);
                };

                await channel.BasicConsumeAsync(queueName, false, consumer);
                Console.WriteLine($"Subscribed to the queue with key {routingKey} (exchange name: {exchangeName})");
                Console.ReadLine();
        }
    }
}