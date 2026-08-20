using System.Text.Json;
using Confluent.Kafka;

namespace TestCDC.API;

public record ProductChange(
    int Id,
    string Name,
    decimal Price,
    [property: System.Text.Json.Serialization.JsonPropertyName("__deleted")]
    string Deleted);

public class ProductCdcConsumer : BackgroundService
{
    private readonly ILogger<ProductCdcConsumer> _logger;
    private const string Topic = "appdb.public.products";

    public ProductCdcConsumer(ILogger<ProductCdcConsumer> logger) => _logger = logger;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Chạy trên thread riêng vì Consume() là blocking call
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:29092",
            GroupId = "dotnet-cdc-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // tự commit sau khi xử lý xong -> at-least-once
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topic);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(ct);

                // Tombstone (value null) khi record bị xóa và tombstones bật -> bỏ qua
                if (cr.Message.Value is null)
                {
                    consumer.Commit(cr);
                    continue;
                }

                var change = JsonSerializer.Deserialize<ProductChange>(cr.Message.Value);
                if (change is null) { consumer.Commit(cr); continue; }

                if (change.Deleted == "true")
                    _logger.LogInformation("DELETE product {Id}", change.Id);
                else
                    _logger.LogInformation("UPSERT product {Id} - {Name} - {Price}",
                        change.Id, change.Name, change.Price);

                // TODO: đẩy vào read-model / handler của bạn ở đây

                consumer.Commit(cr); // commit sau khi xử lý thành công
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error");
            }
            catch (OperationCanceledException) { break; }
        }

        consumer.Close();
    }
}
