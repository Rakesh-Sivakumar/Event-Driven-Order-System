using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Shared.Contracts;
using System.Text.Json;

namespace Inventory.Worker
{
    public class Worker : BackgroundService
    {
        private readonly string _connectionString = "Server=localhost,1433;Database=OrderDb;User Id=sa;Password=Admin@Pass123;TrustServerCertificate=True";
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "inverntory-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe("payments");

            Console.WriteLine("Inventory Worker started....");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    Console.WriteLine("Payment event received");

                    var paymentEvent = JsonSerializer.Deserialize<PaymentCompletedEvent>(result.Message.Value);

                    // VALIDATION
                    if (paymentEvent == null ||
                        string.IsNullOrEmpty(paymentEvent.ProductName) ||
                        paymentEvent.Quantity <= 0 ||
                        string.IsNullOrEmpty(paymentEvent.EventId))
                    {
                        Console.WriteLine("Invalid event. Skipping...");
                        continue;
                    }

                    using var connection = new SqlConnection(_connectionString);
                    await connection.OpenAsync(stoppingToken);

                    Console.WriteLine($"Checking event: {paymentEvent.EventId}");

                    // IDEMPOTENCY CHECK
                    var checkCmd = new SqlCommand("SELECT COUNT(1) FROM ProcessedEvents WHERE EventId = @EventId", connection);
                    checkCmd.Parameters.AddWithValue("@EventId", paymentEvent.EventId);
                        
                        var exist = (int)await checkCmd.ExecuteScalarAsync();

                        if (exist > 0)
                        {
                            Console.WriteLine("Event already processed. Skipping...");
                            continue;
                        }

                    Console.WriteLine($"Checking event: {paymentEvent.EventId}");
                    var command = new SqlCommand(
                            "UPDATE Inventory SET Stock = Stock - @Qty WHERE ProductName = @Name", connection);
                    command.Parameters.AddWithValue("@Qty", paymentEvent.Quantity);
                    command.Parameters.AddWithValue("@Name", paymentEvent.ProductName);

                    // SAVE PROCESSED EVENT (ONLY IF SUCCESS)
                    int retryCount = 3;
                    int rows = 0;

                    while (retryCount > 0)
                    {
                        try
                        {
                            rows = await command.ExecuteNonQueryAsync(stoppingToken);
                            Console.WriteLine($"Stock updated. Rows affected: {rows}");
                            break;
                        }
                        catch (Exception ex)
                        {
                            retryCount--;
                            Console.WriteLine($"DB Error: {ex.Message}. Retrying...");

                            if (retryCount == 0)
                            {
                                Console.WriteLine("Failed after retries");
                                throw;
                            }

                            await Task.Delay(1000, stoppingToken);
                        }
                    }
                    // SAVE PROCESSED EVENT (ONLY IF SUCCESS)
                    if (rows > 0)
                    {
                        var insertCmd = new SqlCommand("INSERT INTO ProcessedEvents (EventId) VALUES (@EventId)", connection);
                        insertCmd.Parameters.AddWithValue("@EventId", paymentEvent.EventId);
                        await insertCmd.ExecuteNonQueryAsync(stoppingToken);
                        Console.WriteLine("Event recorded as processed");
                    }      
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"KafkaError: {ex.Error.Reason}");
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"General Error: {ex.Message}");
                }
            }

            consumer.Close();
        }
    }
}
