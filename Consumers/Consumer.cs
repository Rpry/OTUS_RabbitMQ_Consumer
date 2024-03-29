using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Consumers
{
    public static class Consumer
    {
        public static void Register(IModel channel, string exchangeName, string queueName, string routingKey)
        {
                channel.BasicQos(0, 10, false);
                channel.QueueDeclare(queueName, false, false, false, null);
                channel.QueueBind(queueName, exchangeName, routingKey, null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, e) =>
                {
                    //throw new Exception("Error has occured");
                    var body = e.Body;
                    var message = JsonSerializer.Deserialize<MessageDto>(Encoding.UTF8.GetString(body.ToArray()));
                    Console.WriteLine($"{DateTime.Now} Received message: {message.Content}");
                    channel.BasicAck(e.DeliveryTag, false);
                    Thread.Sleep(TimeSpan.FromSeconds(2)); // Имитация долгой обработки
                };

                channel.BasicConsume(queueName, false, consumer);
                Console.WriteLine($"Subscribed to the queue with key {routingKey} (exchange name: {exchangeName})");
                Console.ReadLine();
        }
    }
}