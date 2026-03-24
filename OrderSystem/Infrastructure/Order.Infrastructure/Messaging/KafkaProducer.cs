using Confluent.Kafka;
using System.Text.Json;

namespace Order.Infrastructure.Messaging
{
    public class KafkaProducer
    {
        private readonly ProducerConfig _config;

        public KafkaProducer()
        {
            _config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }

        public async Task ProducerAsync(string topic, object message)
        {
            using var producer = new ProducerBuilder<Null, string>(_config).Build();

            var jsonMessage = JsonSerializer.Serialize(message);

            await producer.ProduceAsync(topic, new Message<Null, string>
            {
                Value = jsonMessage
            });
        }
    }
}
