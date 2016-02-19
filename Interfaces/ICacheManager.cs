using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Baris.Common.Cache.Interfaces
{
    /// <summary>
    /// Interface for managing the different cache clients
    /// </summary>
    public interface ICacheManager
    {
        /// <summary>
        /// Gets the cache client.
        /// </summary>
        /// <value>
        /// The cache client.
        /// </value>
        ICacheClient CacheClient { get; }

        /// <summary>
        /// Gets the second cache client.
        /// </summary>
        /// <value>
        /// The second cache client.
        /// </value>
        ICacheClient SecondCacheClient { get; }

        /// <summary>
        /// Tries to retrieve value from cache.
        /// If value is not found, the function is executed and the return value is stored in cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="method">The method.</param>
        /// <param name="key">The key.</param>
        /// <param name="cacheNull">if set to <c>true</c> [cache null].</param>
        /// <returns></returns>
        T TryCache<T>(Func<T> method, string key, bool cacheNull = true);

        /// <summary>
        /// Tries to retrieve value from cache.
        /// If value is not found, the function is executed and the return value is stored in cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="method">The method.</param>
        /// <param name="key">The key.</param>
        /// <param name="expireInMinutes">The expire in minutes.</param>
        /// <param name="cacheNull">if set to <c>true</c> [cache null].</param>
        /// <returns></returns>
        T TryCache<T>(Func<T> method, string key, int expireInMinutes, bool cacheNull = true);

        /// <summary>
        /// Tries to retrieve value from cache.
        /// If value is not found, the function is executed and the return value is stored in cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="method">The method.</param>
        /// <param name="key">The key.</param>
        /// <param name="cacheNull">if set to <c>true</c> [cache null].</param>
        /// <returns></returns>
        T TryCacheToSecondClient<T>(Func<T> method, string key, bool cacheNull = true);

        /// <summary>
        /// Tries to retrieve value from cache.
        /// If value is not found, the function is executed and the return value is stored in cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="method">The method.</param>
        /// <param name="key">The key.</param>
        /// <param name="expireInMinutes">The expire in minutes.</param>
        /// <param name="cacheNull">if set to <c>true</c> [cache null].</param>
        /// <returns></returns>
        T TryCacheToSecondClient<T>(Func<T> method, string key, int expireInMinutes, bool cacheNull = true);

        /// <summary>
        /// Reads from the cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="convertedValue">The converted value.</param>
        /// <returns></returns>
        bool FromCache<T>(string key, out T convertedValue);

        /// <summary>
        /// Reads from the second cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="convertedValue">The converted value.</param>
        /// <returns></returns>
        bool FromSecondCache<T>(string key, out T convertedValue);

        /// <summary>
        /// Sets value To the cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expireInMinutes">The expire in minutes.</param>
        /// <returns></returns>
        bool ToCache<T>(string key, T value, int expireInMinutes);

        /// <summary>
        /// Sets value To the cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        bool ToCache<T>(string key, T value);

        /// <summary>
        /// Sets value To the second cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expireInMinutes">The expire in minutes.</param>
        /// <returns></returns>
        bool ToSecondCache<T>(string key, T value, int expireInMinutes);

        /// <summary>
        /// Sets value To the second cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        bool ToSecondCache<T>(string key, T value);

        /// <summary>
        /// Gets a value indicating whether this instance is enabled.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is enabled; otherwise, <c>false</c>.
        /// </value>
        bool IsEnabled { get; }
    }
}
