using System.Text.Json;
using StackExchange.Redis;

namespace CdcConsumer;

// Clean read-model shape written to the cache (drops the Debezium __deleted flag).
public record ProductView(int Id, string Name, decimal Price);

// Debezium message key ({"id":N}) — used to identify the row on delete tombstones.
public record ProductKey(int Id);

public interface IProductCache
{
    Task UpsertAsync(ProductView product, CancellationToken ct = default);
    Task RemoveAsync(int id, CancellationToken ct = default);
}

// Redis-backed read model kept in sync by CDC. Values are plain JSON strings so
// other services/languages can read them directly. No TTL: entries persist until
// a CDC change event updates or removes them.
// Note: StackExchange.Redis operations don't take a CancellationToken (they rely
// on connection-level timeouts), so the ct params are unused here.
public class ProductCache(IConnectionMultiplexer mux) : IProductCache
{
    private readonly IDatabase _db = mux.GetDatabase();

    private static RedisKey Key(int id) => $"product:{id}";

    public Task UpsertAsync(ProductView product, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(product);
        return _db.StringSetAsync(Key(product.Id), json);
    }

    public Task RemoveAsync(int id, CancellationToken ct = default)
        => _db.KeyDeleteAsync(Key(id));
}
