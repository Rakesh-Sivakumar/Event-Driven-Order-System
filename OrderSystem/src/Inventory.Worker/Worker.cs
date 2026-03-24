using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Shared.Contracts;
using StackExchange.Redis;
using System.Text.Json;

namespace Inventory.Worker
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
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "inverntory-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe("payments");

            _logger.LogInformation("Inventory Worker started....");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    _logger.LogInformation("Payment event received");

                    var paymentEvent = JsonSerializer.Deserialize<PaymentCompletedEvent>(result.Message.Value);

                    // VALIDATION
                    if (paymentEvent == null ||
                        string.IsNullOrEmpty(paymentEvent.ProductName) ||
                        paymentEvent.Quantity <= 0 ||
                        string.IsNullOrEmpty(paymentEvent.EventId))
                    {
                        _logger.LogInformation("Invalid event. Skipping...");
                        continue;
                    }

                    using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
                    await connection.OpenAsync(stoppingToken);

                    _logger.LogInformation($"Checking event: {paymentEvent.EventId}");

                    // IDEMPOTENCY CHECK
                    var checkCmd = new SqlCommand("SELECT COUNT(1) FROM ProcessedEvents WHERE EventId = @EventId", connection);
                    checkCmd.Parameters.AddWithValue("@EventId", paymentEvent.EventId);
                        
                        var exist = (int)await checkCmd.ExecuteScalarAsync();

                        if (exist > 0)
                        {
                            _logger.LogInformation("Event already processed. Skipping...");
                            continue;
                        }

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
                            _logger.LogInformation($"Stock updated. Rows affected: {rows}");
                            break;
                        }
                        catch (Exception ex)
                        {
                            retryCount--;
                            _logger.LogError($"DB Error: {ex.Message}. Retrying...");

                            if (retryCount == 0)
                            {
                                _logger.LogError("Failed after retries");
                                throw;
                            }

                            await Task.Delay(1000, stoppingToken);
                        }
                    }

                    //Redis invalidate
                    var redis = ConnectionMultiplexer.Connect("localhost:6379");
                    var cacheDb = redis.GetDatabase();

                    await cacheDb.KeyDeleteAsync($"inventory:{paymentEvent.ProductName}");

                    _logger.LogInformation("Cache invalidated");


                    // SAVE PROCESSED EVENT (ONLY IF SUCCESS)
                    if (rows > 0)
                    {
                        var insertCmd = new SqlCommand("INSERT INTO ProcessedEvents (EventId) VALUES (@EventId)", connection);
                        insertCmd.Parameters.AddWithValue("@EventId", paymentEvent.EventId);
                        await insertCmd.ExecuteNonQueryAsync(stoppingToken);
                        _logger.LogInformation("Event recorded as processed");
                    }      
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError($"KafkaError: {ex.Error.Reason}");
                }
                catch(Exception ex)
                {
                    _logger.LogWarning($"General Error: {ex.Message}");
                }
            }

            consumer.Close();
        }
    }
}
