using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

namespace CdcConsumer;

public record ProductChange(
    int Id,
    string Name,
    decimal Price,
    [property: System.Text.Json.Serialization.JsonPropertyName("__deleted")]
    string Deleted);

public class ProductCdcConsumer(ILogger<ProductCdcConsumer> logger) : BackgroundService
{
    private readonly JsonSerializerOptions options = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };
    private readonly ILogger<ProductCdcConsumer> _logger = logger;
    private const string Topic = "appdb.public.products";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Chạy trên thread riêng vì Consume() là blocking call
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:29092",
            GroupId = "dotnet-cdc-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // tự commit sau khi xử lý xong -> at-least-once
            BrokerAddressFamily = BrokerAddressFamily.V4
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

                ProductChange? change;
                try
                {
                    change = JsonSerializer.Deserialize<ProductChange>(cr.Message.Value, options);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Bad message at offset {Offset}: {Raw}",
                        cr.Offset, cr.Message.Value);
                    // commit offset / đẩy sang dead-letter tùy chiến lược
                    continue;
                }
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
