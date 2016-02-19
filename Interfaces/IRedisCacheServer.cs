using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ServiceStack.Redis;
using Baris.Common.Cache.Clients;
using Baris.Common.Cache.Server;

namespace Baris.Common.Cache.Interfaces
{
    public interface IRedisCacheServer
    {
        /// <summary>
        /// Wraps the async method, that when called async health check and failover to the new master if required.
        /// If the master is not changed, the slave will become the master and write to database the new master as itself.
        /// </summary>
        Task FailOverIfRequiredAsync(RedisCacheClient currentRedisCacheClient, string failoverStarterKey);

        /// <summary>
        /// Gets the slave info async.
        /// </summary>
        /// <param name="redisSlave">The redis slave.</param>
        /// <param name="currentRedisCacheClient">The current redis cache client.</param>
        /// <returns></returns>
        Task<RedisSlaveInfo> GetSlaveInfoAsync(IRedisNativeClient redisSlave, RedisCacheClient currentRedisCacheClient);

        /// <summary>
        /// Reads the latest master in database async.
        /// </summary>
        /// <returns></returns>
        string ReadLatestMasterInDatabase();

        /// <summary>
        /// Sets the master in app domain load.
        /// </summary>
        /// <param name="currentRedisCacheClient">The current redis cache client.</param>
        /// <param name="writerRedisHostFromConfig">The writer redis host from config.</param>
        /// <param name="writerRedisPortFromConfig">The writer redis port from config.</param>
        void SetMasterInAppDomainLoad(RedisCacheClient currentRedisCacheClient, string writerRedisHostFromConfig,
            int writerRedisPortFromConfig);
    }
}
