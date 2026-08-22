using Microsoft.Extensions.Caching.Hybrid;

namespace CdcConsumer;

// Clean read-model shape written to the cache (drops the Debezium __deleted flag).
public record ProductView(int Id, string Name, decimal Price);

// Debezium message key ({"id":N}) — used to identify the row on delete tombstones.
public record ProductKey(int Id);

public interface IProductCache
{
    ValueTask UpsertAsync(ProductView product, CancellationToken ct = default);
    ValueTask RemoveAsync(int id, CancellationToken ct = default);
}

// Dedicated cache service wrapping HybridCache (L1 in-memory + L2 Redis).
// All products share the "products" tag so the collection can be invalidated
// together via cache.RemoveByTagAsync("products").
public class ProductCache(HybridCache cache) : IProductCache
{
    private static readonly string[] Tags = ["products"];

    // Read-model kept in sync by CDC -> entries persist until a change event
    // updates/removes them (don't let the default ~5 min expiration drop them).
    private static readonly HybridCacheEntryOptions EntryOptions = new()
    {
        Expiration = TimeSpan.FromDays(365),
        LocalCacheExpiration = TimeSpan.FromMinutes(5)
    };

    private static string Key(int id) => $"product:{id}";

    public ValueTask UpsertAsync(ProductView product, CancellationToken ct = default)
        => cache.SetAsync(Key(product.Id), product, EntryOptions, Tags, ct);

    public ValueTask RemoveAsync(int id, CancellationToken ct = default)
        => cache.RemoveAsync(Key(id), ct);
}
