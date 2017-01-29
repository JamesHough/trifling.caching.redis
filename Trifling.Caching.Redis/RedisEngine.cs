namespace Trifling.Caching.Redis
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;

    using Microsoft.Extensions.Logging;

    using StackExchange.Redis;

    using Trifling.Caching.Interfaces;

    public class RedisEngine : ICacheEngine
    {
        /// <summary>
        /// A value indicating if the Initialize method has already been called.
        /// </summary>
        private bool _initialized = false;

        /// <summary>
        /// The open connection to Redis server.
        /// </summary>
        private static Lazy<ConnectionMultiplexer> RedisConnection;

        /// <summary>
        /// The given configuration options for connecting to Redis.
        /// </summary>
        private CacheEngineConfiguration _cacheEngineConfiguration;

        /// <summary>
        /// The given logger that will be used by Redis server to report on events.
        /// </summary>
        private readonly ILogger _cacheEventLogger;

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
            if (this._initialized && RedisConnection.IsValueCreated)
            {
                RedisConnection.Value.Close();
            }

            // if the configuration is null then use default values (local Redis server is assumed).
            this._cacheEngineConfiguration =
                cacheEngineConfiguration ?? new CacheEngineConfiguration { Server = "localhost", Port = 6379 };
            
            // set-up the lazy loader which will make the connection when needed.
            RedisConnection = new Lazy<ConnectionMultiplexer>(
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
        /// Stores the given value in the Redis cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the entry to create or overwrite in the cache.</param>
        /// <param name="value">The data to store in the cache.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        public void Cache(string cacheEntryKey, byte[] value, TimeSpan expiry)
        {
            var db = this.GetDatabase();

            db.StringSet(cacheEntryKey, value, expiry);
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

        /// <summary>
        /// Fetches a stored value from the cache. If the key was found then the value is returned. If not 
        /// found then a null is returned.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located value from the cache if the key was found. Otherwise null.</returns>
        public byte[] Retrieve(string cacheEntryKey)
        {
            var db = this.GetDatabase();

            return db.StringGet(cacheEntryKey);
        }

        /// <summary>
        /// Gets the active Redis database (usually 0, depending on the configuration this can be anything up to 16).
        /// </summary>
        /// <returns>Returns a database connection for performing Redis server calls.</returns>
        private IDatabase GetDatabase()
        {
            return RedisConnection.Value.GetDatabase();
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
    }
}
