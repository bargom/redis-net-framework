using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;

namespace Baris.Common.Cache.RedisNative
{
    public class RedisClientNullableFactory : IRedisClientFactory
    {
        public static RedisClientNullableFactory Instance = new RedisClientNullableFactory();

        public RedisClient CreateRedisClient(string host, int port)
        {
            return new RedisClientNullable(host, port);
        }
    }
}
