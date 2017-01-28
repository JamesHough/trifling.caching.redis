namespace Trifling.Caching.Redis
{
    using System;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RedisCacheIntegrationTests
    {

        [TestMethod]
        public void RedisCacheIntegrationTests_Cache_WhenValueCached_ThenSameValueRetrieved()
        {
            // ----- Arrange -----
            var data = new byte[] { 59, 58, 57, 56, 55, 54, 53, 52, 51, 90, 89, 88, 87, 86, 85 };
            var cacheKey = "15bytes-cache-retrieve";

            var redis = new RedisEngine();
            redis.Initialise(null);

            // ----- Act -----
            redis.Cache(cacheKey, data, TimeSpan.FromSeconds(5d));

            var retrieved = redis.Retrieve(cacheKey);

            // ----- Asset -----
            Assert.IsNotNull(retrieved);
            Assert.AreEqual(data.Length, retrieved.Length);
            Assert.AreNotSame(data, retrieved, "the engine cheated and returned the exact same instance.");
            for (var i = 0; i < data.Length; i++)
            {
                Assert.AreEqual(data[i], retrieved[i], "expected array values did not match at " + i);
            }
        }
        
        [TestMethod]
        public void RedisCacheIntegrationTests_Remove_WhenRemoveExistingCacheValue_ThenReturnsTrue()
        {
            // ----- Arrange -----
            var data = new byte[] { 61, 71, 81, 91, 101, 111, 121, 131, 141, 151, 161, 171, 181, 191, 120, 121, 122, 123, 124 };
            var cacheKey = "19bytes-cache-remove";

            var redis = new RedisEngine();
            redis.Initialise(null);

            // ----- Act -----
            redis.Cache(cacheKey, data, TimeSpan.FromSeconds(25d));

            var removed = redis.Remove(cacheKey);

            // ----- Asset -----
            Assert.IsTrue(removed);
        }

        [TestMethod]
        public void RedisCacheIntegrationTests_Remove_WhenRemoveNonExistentCacheKey_ThenReturnsFalse()
        {
            // ----- Arrange -----
            var cacheKey = "neverexisted-remove";

            var redis = new RedisEngine();
            redis.Initialise(null);

            // ----- Act -----
            var removed = redis.Remove(cacheKey);

            // ----- Asset -----
            Assert.IsFalse(removed);
        }

        [TestMethod]
        public void RedisCacheIntegrationTests_Retrieve_WhenNonExistentCacheKey_ThenReturnsNull()
        {
            // ----- Arrange -----
            var cacheKey = "neverexisted-retrieve";

            var redis = new RedisEngine();
            redis.Initialise(null);

            // ----- Act -----
            var retrieved = redis.Retrieve(cacheKey);

            // ----- Asset -----
            Assert.IsNull(retrieved);
        }

    }
}
