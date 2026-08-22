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
    Task<IReadOnlyList<ProductView>> GetAllAsync(CancellationToken ct = default);
}

// Redis-backed read model kept in sync by CDC. Values are plain JSON strings so
// other services/languages can read them directly. No TTL: entries persist until
// a CDC change event updates or removes them.
// Note: StackExchange.Redis operations don't take a CancellationToken (they rely
// on connection-level timeouts), so the ct params are unused here.
public class ProductCache(IConnectionMultiplexer mux) : IProductCache
{
    private readonly IDatabase _db = mux.GetDatabase();

    // Set of live product ids, so the whole collection can be listed (SMEMBERS + MGET).
    private static readonly RedisKey IndexKey = "products:index";

    private static RedisKey Key(int id) => $"product:{id}";

    public async Task UpsertAsync(ProductView product, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(product);
        // Atomic: the value write and the index add land together (MULTI/EXEC),
        // so a concurrent reader never sees a value without its index entry.
        var tran = _db.CreateTransaction();
        _ = tran.StringSetAsync(Key(product.Id), json);
        _ = tran.SetAddAsync(IndexKey, product.Id);
        await tran.ExecuteAsync();
    }

    public async Task RemoveAsync(int id, CancellationToken ct = default)
    {
        // Atomic: drop the value and its index entry together.
        var tran = _db.CreateTransaction();
        _ = tran.KeyDeleteAsync(Key(id));
        _ = tran.SetRemoveAsync(IndexKey, id);
        await tran.ExecuteAsync();
    }

    public async Task<IReadOnlyList<ProductView>> GetAllAsync(CancellationToken ct = default)
    {
        var ids = await _db.SetMembersAsync(IndexKey);
        if (ids.Length == 0) return [];

        var keys = Array.ConvertAll(ids, id => (RedisKey)$"product:{(int)id}");
        var values = await _db.StringGetAsync(keys);

        var products = new List<ProductView>(values.Length);
        foreach (var value in values)
        {
            // Tolerate a stale index id whose value key is already gone.
            if (value.IsNullOrEmpty) continue;
            var product = JsonSerializer.Deserialize<ProductView>((string)value!);
            if (product is not null) products.Add(product);
        }
        return products;
    }
}
