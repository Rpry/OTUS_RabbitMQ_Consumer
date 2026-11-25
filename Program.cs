using System.Threading.Tasks;
using Consumer.Settings;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace Consumer
{
    class Program
    {
        private static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();

            var consumerNumber = int.Parse(args[0]);

            var connection = await GetRabbitConnection(configuration);
            var channel = await connection.CreateChannelAsync();

            await Consumers.Consumer.Register(channel, $"exchange.direct", $"queue.direct_{consumerNumber}", $"cars.{consumerNumber}");
            //await Consumers.Consumer.Register(channel, $"exchange.fanout", $"queue.fanout_{consumerNumber}", $"cars.{consumerNumber}");
            //await Consumers.Consumer.Register(channel, $"exchange.topic", $"queue.topic_{consumerNumber}",  consumerNumber > 2 ? "*.1": $"cars.{consumerNumber}");
        }
        
        private static async Task<IConnection> GetRabbitConnection(IConfiguration configuration)
        {
            var rmqSettings = configuration.Get<ApplicationSettings>().RmqSettings;
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = rmqSettings.Host,
                VirtualHost = rmqSettings.VHost,
                UserName = rmqSettings.Login,
                Password = rmqSettings.Password,
            };
            IConnection conn = await factory.CreateConnectionAsync();
            return conn;
        }
    }
}
