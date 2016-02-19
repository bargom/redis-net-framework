using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using log4net;
using ServiceStack.Text;
using Baris.Common.Cache.Interfaces;

namespace Baris.Common.Cache.RedisNative
{
    /// <summary>
    /// Null support for redis
    /// </summary>
    public interface IRedisClientNullable : ServiceStack.Redis.IRedisClient
    {
        /// <summary>
        /// Gets the specified key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        bool Get<T>(string key, out T value);

        /// <summary>
        /// Sets as null.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="expire">The expire.</param>
        /// <returns></returns>
        bool SetAsNull(string key, TimeSpan expire);

        /// <summary>
        /// Adds as null.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="expire">The expire.</param>
        /// <returns></returns>
        bool AddAsNull(string key, TimeSpan expire);
    }
}
