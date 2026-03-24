using StackExchange.Redis;

namespace Order.Infrastructure.Caching
{
    public class RedisService
    {
        private readonly IDatabase _db;

        public RedisService()
        {
            var redis = ConnectionMultiplexer.Connect("localhost:6379");
            _db = redis.GetDatabase();
        }

        public async Task<string> GetAsync(string key)
        {
            return await _db.StringGetAsync(key);
        }

        public async Task SetAsync(string key, string value)
        {
            await _db.StringSetAsync(key, value, TimeSpan.FromMinutes(10));
        }
        public async Task RemoveAsync(string key)
        {
            await _db.KeyDeleteAsync(key);
        }
    }
}
