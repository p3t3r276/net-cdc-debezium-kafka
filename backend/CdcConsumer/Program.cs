using CdcConsumer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

var redisConn = builder.Configuration["Redis:Connection"] ?? "localhost:6379";
builder.Services.AddStackExchangeRedisCache(o => o.Configuration = redisConn);
builder.Services.AddHybridCache();
builder.Services.AddSingleton<IProductCache, ProductCache>();

builder.Services.AddHostedService<ProductCdcConsumer>();


var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// app.UseHttpsRedirection();

app.Run();
