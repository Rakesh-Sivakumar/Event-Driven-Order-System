using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Shared.Contracts;
using System.Text.Json;

namespace Payment.Worker
{
    public class Worker : BackgroundService
    {
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

            Console.WriteLine("Payment Worker started....");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    Console.WriteLine("Message received from Kafka");

                    var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

                    if (orderEvent == null)
                    {
                        Console.WriteLine("Deserialization failed");
                        continue;
                    }

                    Console.WriteLine($"Processing payment for Order: {orderEvent.OrderId}");

                    try
                    {
                        using (var connection = new SqlConnection(
                            "Server=localhost,1433;Database=OrderDb;User Id=sa;Password=Admin@Pass123;TrustServerCertificate=True"))
                        {
                            await connection.OpenAsync(stoppingToken);
                            Console.WriteLine("DB Connected");

                            var command = new SqlCommand(
                                "UPDATE Orders SET Status = 'Paid' WHERE Id = @Id",
                                connection);

                            command.Parameters.AddWithValue("@Id", orderEvent.OrderId);

                            var rows = await command.ExecuteNonQueryAsync(stoppingToken);

                            Console.WriteLine($"Rows affected: {rows}");
                        }
                    }
                    catch (Exception dbEx)
                    {
                        Console.WriteLine($"DB ERROR: {dbEx.Message}");
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

                    Console.WriteLine("Payment event published to Kafka");
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Kafka Error: {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"General Error: {ex.Message}");
                }
            }

            consumer.Close();

        }
    }
}
