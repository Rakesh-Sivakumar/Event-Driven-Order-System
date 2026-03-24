using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Shared.Contracts;
using System.Text.Json;

namespace Payment.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var ConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "payment-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(ConsumerConfig).Build();
            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            consumer.Subscribe("orders");

            _logger.LogInformation("Payment Worker started....");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    _logger.LogInformation("Message received from Kafka");

                    var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

                    if (orderEvent == null)
                    {
                        _logger.LogWarning("Deserialization failed");
                        continue;
                    }

                    _logger.LogInformation($"Processing payment for Order: {orderEvent.OrderId}");

                    try
                    {
                        using (var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
                        {
                            await connection.OpenAsync(stoppingToken);
                            _logger.LogInformation("DB Connected");

                            var command = new SqlCommand(
                                "UPDATE Orders SET Status = 'Paid' WHERE Id = @Id",
                                connection);

                            command.Parameters.AddWithValue("@Id", orderEvent.OrderId);

                            var rows = await command.ExecuteNonQueryAsync(stoppingToken);

                            _logger.LogInformation($"Rows affected: {rows}");
                        }
                    }
                    catch (Exception dbEx)
                    {
                        _logger.LogError($"DB ERROR: {dbEx.Message}");
                    }

                    // PRODUCE NEXT EVENT (Payment Completed)
                    var paymentEvent = new PaymentCompletedEvent
                    {
                        EventId = Guid.NewGuid().ToString(),
                        OrderId = orderEvent.OrderId,
                        ProductName = orderEvent.ProductName,
                        Quantity =  orderEvent.Quantity,
                        Status = "Paid"
                    };

                    var json = JsonSerializer.Serialize(paymentEvent);

                    await producer.ProduceAsync("payments", new Message<Null, string>
                    {
                        Value = json
                    }, stoppingToken);

                    _logger.LogInformation("Payment event published to Kafka");
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError($"Kafka Error: {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"General Error: {ex.Message}");
                }
            }

            consumer.Close();

        }
    }
}
