// <copyright company="James Hough">
//   Copyright (c) James Hough. Licensed under MIT License - refer to LICENSE file
// </copyright>
namespace Trifling.Caching.Redis
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Microsoft.Extensions.Logging;

    using StackExchange.Redis;

    using Trifling.Caching.Interfaces;
    using Trifling.Comparison;

    /// <summary>
    /// An implementation of <see cref="ICacheEngine"/> for caching on a Redis server. 
    /// </summary>
    public class RedisEngine : ICacheEngine
    {
        /// <summary>
        /// The open connection to Redis server.
        /// </summary>
        private static Lazy<ConnectionMultiplexer> redisConnection;

        /// <summary>
        /// The given logger that will be used by Redis server to report on events.
        /// </summary>
        private readonly ILogger _cacheEventLogger;

        /// <summary>
        /// A value indicating if the Initialize method has already been called.
        /// </summary>
        private bool _initialized = false;

        /// <summary>
        /// The given configuration options for connecting to Redis.
        /// </summary>
        private CacheEngineConfiguration _cacheEngineConfiguration;

        /// <summary>
        /// Initialises a new instance of the <see cref="RedisEngine"/> class.  
        /// </summary>
        public RedisEngine()
            : this(null)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="RedisEngine"/> class with the given
        /// event logger for Redis to write to.
        /// </summary>
        /// <param name="cacheEventLogger">A logger which can accept logging information from Redis server.</param>
        public RedisEngine(
            ILogger cacheEventLogger)
        {
            this._cacheEventLogger = cacheEventLogger;
        }

        /// <summary>
        /// Initialises the connection to Redis with the given configuration.
        /// </summary>
        /// <param name="cacheEngineConfiguration">The configuration options for connecting to Redis.</param>
        public void Initialise(CacheEngineConfiguration cacheEngineConfiguration)
        {
            // if the Initialise method was already called, then any existing connection will need to be closed
            // before reconfiguring with the new configuration.
            if (this._initialized && redisConnection.IsValueCreated)
            {
                redisConnection.Value.Close();
            }

            // if the configuration is null then use default values (local Redis server is assumed).
            this._cacheEngineConfiguration =
                cacheEngineConfiguration ?? new CacheEngineConfiguration { Server = "localhost", Port = 6379 };

            // set-up the lazy loader which will make the connection when needed.
            redisConnection = new Lazy<ConnectionMultiplexer>(
                () =>
                {
                    var c = ConnectionMultiplexer.Connect(this.GetRedisConfiguration(), this.GetLogEntryTextWriter());

                    // add extra logging of Redis events.
                    // TODO
                    ////c.ErrorMessage += RedisConnection_ErrorMessage;
                    ////c.ConnectionFailed += RedisConnection_ConnectionFailed;
                    return c;
                });

            this._initialized = true;
        }

        /// <summary>
        /// Deletes the cache entry with the matching unique key. If the entry was found and successfully removed
        /// then this will return true.  Otherwise this will return false.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key identifying the cache entry to delete.</param>
        /// <returns>Returns true if the entry was found and successfully removed from the cache. Otherwise false.</returns>
        public bool Remove(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            return db.KeyDelete(cacheEntryKey);
        }

        #region Single value caching

        /// <summary>
        /// Stores the given value in the Redis cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the entry to create or overwrite in the cache.</param>
        /// <param name="value">The data to store in the cache.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the value was successfully cached. Otherwise false.</returns>
        public bool Cache(string cacheEntryKey, byte[] value, TimeSpan expiry)
        {
            var db = this.GetDatabase();

            return db.StringSet(cacheEntryKey, value, expiry);
        }

        /// <summary>
        /// Fetches a stored value from the cache. If the key was found then the value is returned. If not 
        /// found then a null is returned.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located value from the cache if the key was found. Otherwise null.</returns>
        public byte[] Retrieve(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.String))
            {
                return null;
            }

            return db.StringGet(cacheEntryKey);
        }

        #endregion Single value caching

        #region Set caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="setItems"/> as a set in the cache.
        /// </summary>
        /// <remarks>
        /// Items of a set are not guaranteed to retain ordering when retrieved from cache. The
        /// implementation of <see cref="RetrieveSet{T}"/> returns a sorted set even if the input
        /// was not sorted.
        /// </remarks>
        /// <typeparam name="T">The type of object being cached. All items of the set must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="setItems">The individual items to store as a set.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the set was successfully created with all <paramref name="setItems"/> values cached.</returns>
        public bool CacheAsSet<T>(string cacheEntryKey, IEnumerable<T> setItems, TimeSpan expiry)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            // other values must be string convertable
            var stringValues = setItems
                .Select(item => ValueToString(item))
                .ToArray();

            foreach (var item in stringValues)
            {
                allSuccess &= db.SetAdd(cacheEntryKey, item);
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="setItems"/> byte arrays as a set in the cache.
        /// </summary>
        /// <remarks>
        /// Items of a set are not guaranteed to retain ordering when retrieved from cache. The
        /// implementation of <see cref="RetrieveSet"/> returns a sorted set even if the input
        /// was not sorted.
        /// </remarks>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="setItems">The individual items to store as a set.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the set was successfully created with all <paramref name="setItems"/> values cached.</returns>
        public bool CacheAsSet(string cacheEntryKey, IEnumerable<byte[]> setItems, TimeSpan expiry)
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            foreach (var item in setItems)
            {
                allSuccess &= db.SetAdd(cacheEntryKey, item);
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Adds a single new entry into an existing cached set.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All existing items of the set must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and add to.</param>
        /// <param name="value">The new individual item to store in the existing set.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be added to the cached set. Otherwise true.</returns>
        public bool AddToSet<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            // prevent a new set from spawning if it doesn't exist already.
            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            // values must be string convertable
            return db.SetAdd(cacheEntryKey, ValueToString(value));
        }

        /// <summary>
        /// Adds a single new byte array entry into an existing cached set.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and add to.</param>
        /// <param name="value">The new individual item to store in the existing set.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be added to the cached set. Otherwise true.</returns>
        public bool AddToSet(string cacheEntryKey, byte[] value)
        {
            var db = this.GetDatabase();

            // prevent a new set from spawning if it doesn't exist already.
            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            // values which are already serialised should be written as-is.
            return db.SetAdd(cacheEntryKey, (byte[])(object)value);
        }

        /// <summary>
        /// Removes any matching entries with the same value from an existing cached set.
        /// </summary>
        /// <typeparam name="T">The type of objects that are contained in the cached set.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing set and remove.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached set. Otherwise true.</returns>
        public bool RemoveFromSet<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            // the item could not be removed from the set if it's not in cache.
            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            // values must be string convertable
            return db.SetRemove(cacheEntryKey, ValueToString(value));
        }

        /// <summary>
        /// Removes any matching byte array entries with the same value from an existing cached set.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing set and remove.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached set. Otherwise true.</returns>
        public bool RemoveFromSet(string cacheEntryKey, byte[] value)
        {
            var db = this.GetDatabase();

            // the item could not be removed from the set if it's not in cache.
            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return false;
            }

            // values which are already serialised should be compared as-is.
            return db.SetRemove(cacheEntryKey, (byte[])(object)value);
        }

        /// <summary>
        /// Fetches a stored set from the cache and returns it as a set. If the key was found then the set 
        /// is returned. If not found then a null is returned.
        /// </summary>
        /// <remarks>
        /// The returned set is implemented as a <see cref="SortedSet{T}"/>. And may differ from the order
        /// of items stored in the set in Redis.
        /// </remarks>
        /// <typeparam name="T">The type of objects that are contained in the cached set.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located set from the cache if the key was found. Otherwise null.</returns>
        public ISet<T> RetrieveSet<T>(string cacheEntryKey)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return null;
            }
            
            var returnSet = new SortedSet<T>();

            foreach (var setValue in db.SetScan(cacheEntryKey))
            {
                returnSet.Add(ParseValue<T>(setValue));
            }

            return returnSet;
        }

        /// <summary>
        /// Fetches a stored set from the cache and returns it as a set. If the key was found then the set 
        /// is returned. If not found then a null is returned.
        /// </summary>
        /// <remarks>
        /// The returned set is implemented as a <see cref="SortedSet{T}"/> of byte array values. And may 
        /// differ from the order of items stored in the set in Redis.
        /// </remarks>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located set from the cache if the key was found. Otherwise null.</returns>
        public ISet<byte[]> RetrieveSet(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Set))
            {
                return null;
            }

            var returnSet = new SortedSet<byte[]>(BoxedByteArrayComparer.Default);

            foreach (var setValue in db.SetScan(cacheEntryKey))
            {
                returnSet.Add((byte[])(RedisValue)setValue);
            }

            return returnSet;
        }
        
        #endregion Set caching

        #region List caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="listItems"/> values as a list in the cache.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All items of the list must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="listItems">The individual items to store as a list.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the list was successfully created with all <paramref name="listItems"/> values cached.</returns>
        public bool CacheAsList<T>(string cacheEntryKey, IEnumerable<T> listItems, TimeSpan expiry)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                // cannot overwite another key as a list.
                return false;
            }

            foreach (var item in listItems)
            {
                allSuccess &= db.ListRightPush(cacheEntryKey, ValueToString(item)) >= 0;
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="listItems"/> byte arrays as a list in the cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="listItems">The individual byte array values to store as a list.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the list was successfully created with all <paramref name="listItems"/> values cached.</returns>
        public bool CacheAsList(string cacheEntryKey, IEnumerable<byte[]> listItems, TimeSpan expiry)
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                // cannot overwite another key as a list.
                return false;
            }

            foreach (var item in listItems)
            {
                allSuccess &= db.ListRightPush(cacheEntryKey, item) >= 0;
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Fetches a stored list from the cache and returns it as a <see cref="IList{T}"/>. If the key was 
        /// found then the list is returned. If not found then a null is returned.
        /// </summary>
        /// <typeparam name="T">The type of object that was cached in a list.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located list from the cache if the key was found. Otherwise null.</returns>
        public IList<T> RetrieveList<T>(string cacheEntryKey)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return null;
            }

            var length = db.ListLength(cacheEntryKey);
            var returnList = new List<T>((int)length);

            returnList.AddRange(
                db.ListRange(cacheEntryKey)
                    .Select(l => ParseValue<T>(l)));

            return returnList;
        }

        /// <summary>
        /// Fetches a stored list from the cache and returns it as a List of byte array values. If the key was 
        /// found then the list is returned. If not found then a null is returned.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located list from the cache if the key was found. Otherwise null.</returns>
        public IList<byte[]> RetrieveList(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return null;
            }

            var length = db.ListLength(cacheEntryKey);
            var returnList = new List<byte[]>((int)length);

            returnList.AddRange(
                db.ListRange(cacheEntryKey)
                    .Select(x => (byte[])(RedisValue)x));

            return returnList;
        }

        /// <summary>
        /// Appends a new value to the end of an existing cached list.
        /// </summary>
        /// <typeparam name="T">The type of object being appended to the cached list. All items of the list must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="value">The value to append to the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be appended. Otherwise true.</returns>
        public bool AppendToList<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListRightPush(cacheEntryKey, ValueToString(value));
            return true;
        }

        /// <summary>
        /// Appends a new byte array value to the end of an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="value">The value to append to the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be appended. Otherwise true.</returns>
        public bool AppendToList(string cacheEntryKey, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListRightPush(cacheEntryKey, value);
            return true;
        }

        /// <summary>
        /// Injects a new value into an existing cached list at the position specified.
        /// </summary>
        /// <typeparam name="T">The type of object being injected into the cached list. All items of the list must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="index">The zero-based position at which the value must be inserted in the list.</param>
        /// <param name="value">The value to inject into the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be injected. Otherwise true.</returns>
        public bool InjectInList<T>(string cacheEntryKey, long index, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListInsertBefore(cacheEntryKey, index, ValueToString(value));
            return true;
        }

        /// <summary>
        /// Injects a new byte array value into an existing cached list at the position specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="index">The zero-based position at which the value must be inserted in the list.</param>
        /// <param name="value">The byte array value to inject into the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be injected. Otherwise true.</returns>
        public bool InjectInList(string cacheEntryKey, long index, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListInsertBefore(cacheEntryKey, index, value);
            return true;
        }

        /// <summary>
        /// Truncates values from the cached list so that only the values in the range specified remain.
        /// </summary>
        /// <example>
        /// <para>To remove the first two entries, specify <paramref name="firstIndexKept"/>=2 and <paramref name="lastIndexKept"/>=-1.</para>
        /// <para>To remove the last five entries, specify <paramref name="firstIndexKept"/>=0 and <paramref name="lastIndexKept"/>=-6.</para>
        /// <para>To remove the first and last entries, specify <paramref name="firstIndexKept"/>=1 and <paramref name="lastIndexKept"/>=-2.</para>
        /// </example>
        /// <param name="cacheEntryKey">The unique key of the cached list to attempt to shrink.</param>
        /// <param name="firstIndexKept">The zero-based value of the first value from the list that must be kept. Negative 
        /// values refer to the position from the end of the list (i.e. -1 is the last list entry and -2 is the second last entry).</param>
        /// <param name="lastIndexKept">The zero-based value of the last value from the list that must be kept. Negative 
        /// values refer to the position from the end of the list (i.e. -1 is the last list entry and -2 is the second last entry).</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the list cannot be shrunk. Otherwise true.</returns>
        public bool ShrinkList(string cacheEntryKey, long firstIndexKept, long lastIndexKept)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListTrim(cacheEntryKey, firstIndexKept, lastIndexKept);
            return true;
        }

        /// <summary>
        /// Removes any matching entries with the same value from an existing cached list.
        /// </summary>
        /// <typeparam name="T">The type of objects that are contained in the cached list.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cached list to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing list and remove.</param>
        /// <returns>Returns -1 list doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached list. Otherwise returns the number of removed items.</returns>
        public long RemoveFromList<T>(string cacheEntryKey, T value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return -1L;
            }

            return db.ListRemove(cacheEntryKey, ValueToString(value));
        }

        /// <summary>
        /// Removes any matching entries with the same byte array value from an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached list to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing list and remove.</param>
        /// <returns>Returns -1 list doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached list. Otherwise returns the number of removed items.</returns>
        public long RemoveFromList(string cacheEntryKey, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return -1L;
            }

            return db.ListRemove(cacheEntryKey, value);
        }

        /// <summary>
        /// Removes all items from an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that must be cleared.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the list cannot be cleared. Otherwise true.</returns>
        public bool ClearList(string cacheEntryKey)
        {
            // for clearing we can use the shrink method to shrink to an empty length.
            return this.ShrinkList(cacheEntryKey, -1L, 0L);
        }

        #endregion List caching

        #region Dictionary caching

        /// <summary>
        /// Caches the given dictionary of items as a new dictionary in the cache engine.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All values of the dictionary must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which will contain the dictionary.</param>
        /// <param name="dictionaryItems">The items to cache as a dictionary.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the dictionary was successfully created with all <paramref name="dictionaryItems"/> values cached.</returns>
        public bool CacheAsDictionary<T>(string cacheEntryKey, IDictionary<string, T> dictionaryItems, TimeSpan expiry)
            where T : IConvertible
        {
            var db = this.GetDatabase();
            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.Hash))
            {
                // cannot overwrite existing key as a dictionary
                return false;
            }

            foreach (var item in dictionaryItems)
            {
                allSuccess &= db.HashSet(cacheEntryKey, item.Key, ValueToString(item.Value));
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Caches the given dictionary of byte array items as a new dictionary in the cache engine.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which will contain the dictionary.</param>
        /// <param name="dictionaryItems">The items to cache as a dictionary.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the dictionary was successfully created with all <paramref name="dictionaryItems"/> byte array values cached.</returns>
        public bool CacheAsDictionary(string cacheEntryKey, IDictionary<string, byte[]> dictionaryItems, TimeSpan expiry)
        {
            var db = this.GetDatabase();
            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.Hash))
            {
                // cannot overwrite existing cache entry as a dictionary
                return false;
            }

            foreach (var item in dictionaryItems)
            {
                // values which are already serialised should be written as-is.
                allSuccess &= db.HashSet(cacheEntryKey, item.Key, item.Value);
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Adds a new dictionary entry for the given value into an existing cached dictionary with 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <typeparam name="T">The type of object being added to the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary that the 
        /// <paramref name="value"/> will be added to.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being added.</param>
        /// <param name="value">The value to add into the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be added. Otherwise true.</returns>
        public bool AddToDictionary<T>(string cacheEntryKey, string dictionaryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) 
                || (db.KeyType(cacheEntryKey) != RedisType.Hash) 
                || db.HashExists(cacheEntryKey, dictionaryKey))
            {
                return false;
            }

            db.HashSet(cacheEntryKey, dictionaryKey, ValueToString(value));
            return true;
        }

        /// <summary>
        /// Adds a new dictionary entry for the given byte array value into an existing cached dictionary with 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary that the 
        /// <paramref name="value"/> will be added to.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being added.</param>
        /// <param name="value">The byte array value to add into the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be added. Otherwise true.</returns>
        public bool AddToDictionary(string cacheEntryKey, string dictionaryKey, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) 
                || (db.KeyType(cacheEntryKey) != RedisType.Hash)
                || db.HashExists(cacheEntryKey, dictionaryKey))
            {
                return false;
            }

            db.HashSet(cacheEntryKey, dictionaryKey, value);
            return true;
        }

        /// <summary>
        /// Updates an existing dictionary entry with the given value in an existing cached dictionary for 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <typeparam name="T">The type of object being updated in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being updated.</param>
        /// <param name="value">The value to update in the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be found or the value cannot be updated. Otherwise true.</returns>
        public bool UpdateDictionaryEntry<T>(string cacheEntryKey, string dictionaryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey)
                || (db.KeyType(cacheEntryKey) != RedisType.Hash) 
                || !db.HashExists(cacheEntryKey, dictionaryKey))
            {
                return false;
            }

            db.HashSet(cacheEntryKey, dictionaryKey, ValueToString(value));
            return true;
        }

        /// <summary>
        /// Updates an existing dictionary entry with the given byte array value in an existing cached 
        /// dictionary for the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being updated.</param>
        /// <param name="value">The value to update in the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be found or the value cannot be updated. Otherwise true.</returns>
        public bool UpdateDictionaryEntry(string cacheEntryKey, string dictionaryKey, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) 
                || (db.KeyType(cacheEntryKey) != RedisType.Hash)
                || !db.HashExists(cacheEntryKey, dictionaryKey))
            {
                return false;
            }

            db.HashSet(cacheEntryKey, dictionaryKey, value);
            return true;
        }

        /// <summary>
        /// Removes a dictionary entry from an existing cached dictionary for the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being removed.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be removed. Otherwise true.</returns>
        public bool RemoveFromDictionary(string cacheEntryKey, string dictionaryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey)
                || (db.KeyType(cacheEntryKey) != RedisType.Hash) 
                || !db.HashExists(cacheEntryKey, dictionaryKey))
            {
                return false;
            }

            return db.HashDelete(cacheEntryKey, dictionaryKey);
        }

        /// <summary>
        /// Retrieves all entries in a cached dictionary as a new <see cref="IDictionary{TKey, TValue}"/>. 
        /// </summary>
        /// <typeparam name="T">The type of object which was written in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <returns>Returns the located dictionary from the cache if the key was found. Otherwise null.</returns>
        public IDictionary<string, T> RetrieveDictionary<T>(string cacheEntryKey)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Hash))
            {
                return null;
            }

            var returnDictionary = new Dictionary<string, T>();

            foreach (var hashEntry in db.HashScan(cacheEntryKey))
            {
                returnDictionary.Add(hashEntry.Name, ParseValue<T>(hashEntry.Value));
            }

            return returnDictionary;
        }

        /// <summary>
        /// Retrieves all entries in a cached dictionary as a new <see cref="IDictionary{TKey, TValue}"/>. 
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <returns>Returns the located dictionary containing byte array values from the cache if the key was found. Otherwise null.</returns>
        public IDictionary<string, byte[]> RetrieveDictionary(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.Hash))
            {
                return null;
            }

            var returnDictionary = new Dictionary<string, byte[]>();

            foreach (var hashEntry in db.HashScan(cacheEntryKey))
            {
                returnDictionary.Add(hashEntry.Name, hashEntry.Value);
            }

            return returnDictionary;
        }

        /// <summary>
        /// Retrieves a single entry from a cached dictionary located by the <paramref name="dictionaryKey"/>. 
        /// </summary>
        /// <typeparam name="T">The type of object which was written in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being sought.</param>
        /// <param name="value">Returns the value found in the dictionary cache. If not found the default value is returned.</param>
        /// <returns>Returns true if the value was located in the cached dictionary. Otherwise false.</returns>
        public bool RetrieveDictionaryEntry<T>(string cacheEntryKey, string dictionaryKey, out T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey)
                 || (db.KeyType(cacheEntryKey) != RedisType.Hash) 
                 || !db.HashExists(cacheEntryKey, dictionaryKey))
            {
                value = default(T);
                return false;
            }

            value = ParseValue<T>(db.HashGet(cacheEntryKey, dictionaryKey));
            return true;
        }

        /// <summary>
        /// Retrieves a single entry (a byte array) from a cached dictionary located by the <paramref name="dictionaryKey"/>. 
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being sought.</param>
        /// <param name="value">Returns the byte array value found in the dictionary cache. If not found then null is returned.</param>
        /// <returns>Returns true if the value was located in the cached dictionary. Otherwise false.</returns>
        public bool RetrieveDictionaryEntry(string cacheEntryKey, string dictionaryKey, out byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey)
                 || (db.KeyType(cacheEntryKey) != RedisType.Hash) 
                 || !db.HashExists(cacheEntryKey, dictionaryKey))
            {
                value = null;
                return false;
            }

            value = db.HashGet(cacheEntryKey, dictionaryKey);
            return true;
        }

        #endregion Dictionary caching

        #region Queue caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="queuedItems"/> values as a queue in the cache.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All items of the queue must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="queuedItems">The individual items to store as a queue.</param>
        /// <param name="expiry">The time period that the data will be valid. May be set to never expire by setting <see cref="TimeSpan.MaxValue"/>.</param>
        /// <returns>Returns true if the queue was successfully created with all <paramref name="queuedItems"/> values cached.</returns>
        public bool CacheAsQueue<T>(string cacheEntryKey, IEnumerable<T> queuedItems, TimeSpan expiry)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                // cannot overwite another key as a list.
                return false;
            }

            foreach (var item in queuedItems)
            {
                allSuccess &= db.ListRightPush(cacheEntryKey, ValueToString(item)) >= 0;
            }

            if (!existedPreviously && (expiry.Days < 3650))
            {
                // we only set expiry if it is less than 10 years. It allows queues
                // to never expire (e.g. if the caller specifies TimeSpan.MaxValue).
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="queuedItems"/> byte arrays as a queue in the cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="queuedItems">The individual byte array values to store as a queue.</param>
        /// <param name="expiry">The time period that the data will be valid. May be set to never expire by setting <see cref="TimeSpan.MaxValue"/>.</param>
        /// <returns>Returns true if the queue was successfully created with all <paramref name="queuedItems"/> values cached.</returns>
        public bool CacheAsQueue(string cacheEntryKey, IEnumerable<byte[]> queuedItems, TimeSpan expiry)
        {
            var db = this.GetDatabase();

            var existedPreviously = db.KeyExists(cacheEntryKey);
            var allSuccess = true;

            if (existedPreviously && (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                // cannot overwite another key as a list.
                return false;
            }

            foreach (var item in queuedItems)
            {
                allSuccess &= db.ListRightPush(cacheEntryKey, item) >= 0;
            }

            if (!existedPreviously)
            {
                db.KeyExpire(cacheEntryKey, expiry);
            }

            return allSuccess;
        }
        
        /// <summary>
        /// Pushes a new value to the end of an existing cached queue.
        /// </summary>
        /// <typeparam name="T">The type of object being pushed to the cached queue. All items of the queue must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">The value to append to the cached queue.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be pushed to the queue. Otherwise true.</returns>
        public bool PushQueue<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListRightPush(cacheEntryKey, ValueToString(value));
            return true;
        }

        /// <summary>
        /// Pushes a new byte array to the end of an existing cached queue.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">The value to append to the cached queue.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be pushed to the queue. Otherwise true.</returns>
        public bool PushQueue(string cacheEntryKey, byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListRightPush(cacheEntryKey, value);
            return true;
        }

        /// <summary>
        /// Pops the next value in the cached queue and returns the value.
        /// </summary>
        /// <typeparam name="T">The type of the objects stored in the cached queue. All items of the queue must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">Returns the next value from the cached queue. If not found then a default value is returned.</param>
        /// <returns>Returns true if the next value in the cached queue was successfully returned in <paramref name="value"/>. Otherwise false.</returns>
        public bool PopQueue<T>(string cacheEntryKey, out T value)
            where T : IConvertible
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) 
                || (db.KeyType(cacheEntryKey) != RedisType.List)
                || (db.ListLength(cacheEntryKey) < 1L))
            {
                value = default(T);
                return false;
            }

            value = ParseValue<T>(db.ListLeftPop(cacheEntryKey));
            return true;
        }

        /// <summary>
        /// Pops the next byte array in the cached queue and returns the value.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">Returns the next byte array value from the cached queue. If not found then null is returned.</param>
        /// <returns>Returns true if the next value in the cached queue was successfully returned in <paramref name="value"/>. Otherwise false.</returns>
        public bool PopQueue(string cacheEntryKey, out byte[] value)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) 
                || (db.KeyType(cacheEntryKey) != RedisType.List)
                || (db.ListLength(cacheEntryKey) < 1L))
            {
                value = null;
                return false;
            }

            value = (byte[])(RedisValue)db.ListLeftPop(cacheEntryKey);
            return true;
        }

        /// <summary>
        /// Removes all items from an existing cached queue.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue that must be cleared.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the queue cannot be cleared. Otherwise true.</returns>
        public bool ClearQueue(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            if (!db.KeyExists(cacheEntryKey) || (db.KeyType(cacheEntryKey) != RedisType.List))
            {
                return false;
            }

            db.ListTrim(cacheEntryKey, -1, 0);

            return true;
        }

        #endregion Queue caching

        #region Private methods

        /// <summary>
        /// Parses the string stored in Redis as the type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type to which the value must be converted.</typeparam>
        /// <param name="value">The stored redis value to be converted.</param>
        /// <returns>Returns the typed value parsed from the Redis value.</returns>
        private static T ParseValue<T>(RedisValue value)
            where T : IConvertible
        {
            return (T)Convert.ChangeType(value.ToString(), typeof(T), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Converts an object to a string for storing in cache or for comparison to stored strings. 
        /// </summary>
        /// <param name="value">The value to be converted to a string.</param>
        /// <returns>Returns the value in a string formatted string which is round-trip-aware.</returns>
        private static string ValueToString(object value)
        {
            if (value is DateTime)
            {
                // the round-trip format for date time values depends on whether or not there is a time
                // component.  Without time, the format is "yyyy-MM-dd" but with time it is
                // "yyyy-MM-ddTHH:mm:ss.fffffff"
                var dateTimeValue = (DateTime)value;

                return (dateTimeValue.TimeOfDay.Ticks > 0L)
                    ? dateTimeValue.ToString("o", DateTimeFormatInfo.InvariantInfo)
                    : dateTimeValue.ToString("yyyy-MM-dd", DateTimeFormatInfo.InvariantInfo);
            }
            else if (value is double)
            {
                return ((double)value).ToString("r", NumberFormatInfo.InvariantInfo);
            }
            else if (value is float)
            {
                return ((float)value).ToString("r", NumberFormatInfo.InvariantInfo);
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the active Redis database (usually 0, depending on the configuration this can be anything up to 16).
        /// </summary>
        /// <returns>Returns a database connection for performing Redis server calls.</returns>
        private IDatabase GetDatabase()
        {
            return redisConnection.Value.GetDatabase();
        }

        /// <summary>
        /// Builds a Redis <see cref="ConfigurationOptions"/> object from the current instance's
        /// <see cref="_cacheEngineConfiguration"/>. 
        /// </summary>
        /// <returns>Returns a <see cref="ConfigurationOptions"/> instance for connecting to Redis.</returns>
        private ConfigurationOptions GetRedisConfiguration()
        {
            // fix bad server names
            if (string.IsNullOrWhiteSpace(this._cacheEngineConfiguration.Server))
            {
                // assume the server is local
                this._cacheEngineConfiguration.Server = "localhost";
            }

            // fix any invalid port numbers
            if (this._cacheEngineConfiguration.Port <= 0 || this._cacheEngineConfiguration.Port > 65535)
            {
                // reset this bad port to the default for Redis Server.
                this._cacheEngineConfiguration.Port = 6379;
            }

            var connection = new StringBuilder();
            connection.AppendFormat(CultureInfo.InvariantCulture, "{0}:{1}", this._cacheEngineConfiguration.Server, this._cacheEngineConfiguration.Port);

            // go through the list of alternative servers and remove bad entries.
            if (this._cacheEngineConfiguration.AlternativeServers != null)
            {
                foreach (var server in this._cacheEngineConfiguration.AlternativeServers.Keys)
                {
                    if (string.IsNullOrWhiteSpace(server))
                    {
                        // skip bad server names
                        continue;
                    }

                    if (this._cacheEngineConfiguration.AlternativeServers[server] <= 0 
                        || this._cacheEngineConfiguration.AlternativeServers[server] > 65535)
                    {
                        // reset this bad port to the default for Redis Server.
                        this._cacheEngineConfiguration.AlternativeServers[server] = 6379;
                    }

                    connection.AppendFormat(
                        CultureInfo.InvariantCulture,
                        ",{0}:{1}",
                        server,
                        this._cacheEngineConfiguration.AlternativeServers[server]);
                }
            }

            // if the connection timeout was specified then add that to the connection options.
            if (this._cacheEngineConfiguration.ConnectionTimeoutMilliseconds > 0
                && this._cacheEngineConfiguration.ConnectionTimeoutMilliseconds <= 20000)
            {
                connection.AppendFormat(CultureInfo.InvariantCulture, ",connectTimeout={0}", this._cacheEngineConfiguration.ConnectionTimeoutMilliseconds);
            }

            // if the request timeout was specified then add that to the conection options.
            if (this._cacheEngineConfiguration.ResponseTimeoutMilliseconds > 0
                && this._cacheEngineConfiguration.ResponseTimeoutMilliseconds <= 20000)
            {
                connection.AppendFormat(CultureInfo.InvariantCulture, ",syncTimeout={0}", this._cacheEngineConfiguration.ResponseTimeoutMilliseconds);
            }

            // if there are additional parameter options then append them as-is.
            if (!string.IsNullOrWhiteSpace(this._cacheEngineConfiguration.AdditionalConnectionOptions))
            {
                if (!this._cacheEngineConfiguration.AdditionalConnectionOptions.StartsWith(","))
                {
                    connection.Append(',');
                }

                connection.Append(this._cacheEngineConfiguration.AdditionalConnectionOptions);
            }

            // check if "abortConnect" was explicitly set.  If not then set it to false for more stability in single
            // connection, long lifetime setups.
            var connectionString = connection.ToString();
            if (!connectionString.Contains("abortConnect="))
            {
                connection.Append(",abortConnect=false");
                connectionString = connection.ToString();
            }

            return ConfigurationOptions.Parse(connectionString);
        }

        /// <summary>
        /// Wraps the given <see cref="_cacheEventLogger"/> in a Text Writer (provided by 
        /// <see cref="Trifling.Logging.LoggingTextWriter"/> for Redis to use as a logging destination.
        /// </summary>
        /// <returns>Returns an instance of a <see cref="TextWriter"/> for logging. If there is no 
        /// logger available, then returns null.
        /// </returns>
        private TextWriter GetLogEntryTextWriter()
        {
            return (this._cacheEventLogger == null)
                ? null
                : new Trifling.Logging.LoggingTextWriter(this._cacheEventLogger, LogLevel.Trace);
        }

        #endregion Private methods
    }
}
