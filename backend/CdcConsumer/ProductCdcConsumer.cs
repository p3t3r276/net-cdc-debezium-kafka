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

public class ProductCdcConsumer(ILogger<ProductCdcConsumer> logger, IProductCache cache) : BackgroundService
{
    private readonly JsonSerializerOptions options = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };
    private readonly ILogger<ProductCdcConsumer> _logger = logger;
    private readonly IProductCache _cache = cache;
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

                // Tombstone khi DELETE: value có thể là null, chuỗi rỗng, hoặc literal JSON "null".
                // Key luôn có dạng {"id":N} -> xóa khỏi cache dựa trên id trong key.
                if (string.IsNullOrWhiteSpace(cr.Message.Value) || cr.Message.Value == "null")
                {
                    var key = cr.Message.Key is null
                        ? null
                        : JsonSerializer.Deserialize<ProductKey>(cr.Message.Key, options);
                    if (key is null)
                    {
                        consumer.Commit(cr);
                        continue;
                    }

                    try
                    {
                        _logger.LogInformation("DELETE product {Id} (tombstone)", key.Id);
                        _cache.RemoveAsync(key.Id, ct).GetAwaiter().GetResult();
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogError(ex, "Cache delete failed for product {Id} at offset {Offset}",
                            key.Id, cr.Offset);
                        continue; // không commit -> retry ở poll kế tiếp
                    }

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

                try
                {
                    if (change.Deleted == "true")
                    {
                        _logger.LogInformation("DELETE product {Id}", change.Id);
                        _cache.RemoveAsync(change.Id, ct).GetAwaiter().GetResult();
                    }
                    else
                    {
                        _logger.LogInformation("UPSERT product {Id} - {Name} - {Price}",
                            change.Id, change.Name, change.Price);
                        var view = new ProductView(change.Id, change.Name, change.Price);
                        _cache.UpsertAsync(view, ct).GetAwaiter().GetResult();
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Không commit -> message sẽ được xử lý lại ở poll kế tiếp
                    _logger.LogError(ex, "Cache write failed for product {Id} at offset {Offset}",
                        change.Id, cr.Offset);
                    continue;
                }

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
