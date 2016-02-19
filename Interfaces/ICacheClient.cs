using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Baris.Common.Cache.Interfaces
{
    public interface ICacheClient : IDisposable
    {
        /// <summary>
        /// Checks the existance of specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        bool Exists(string key);

        /// <summary>
        /// Gets the value with the specified key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <returns>Returns the default(T) if value not found or CANNOT BE CONVERTED into T!! Use in caution</returns>
        T Get<T>(string key);

        /// <summary>
        /// Gets the value with the specified key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>True if the value is found</returns>
        bool Get<T>(string key, out T value);

        /// <summary>
        /// Adds the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        bool Add<T>(string key, T value);

        /// <summary>
        /// Sets the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        bool Set<T>(string key, T value);

        /// <summary>
        /// Adds the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expiresInMinutes">The expires in.</param>
        /// <returns></returns>
        bool Add<T>(string key, T value, int expiresInMinutes);

        /// <summary>
        /// Sets the specified value withkey.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expiresInMinutes">The expires in.</param>
        /// <returns></returns>
        bool Set<T>(string key, T value, int expiresInMinutes);

        /// <summary>
        /// Adds the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="task">The task if it is async.</param>
        /// <returns></returns>
        bool Add<T>(string key, T value, out Task task);

        /// <summary>
        /// Sets the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="task">The task if it is async.</param>
        /// <returns></returns>
        bool Set<T>(string key, T value, out Task task);

        /// <summary>
        /// Adds the specified value with key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expiresInMinutes">The expires in.</param>
        /// <param name="task">The task if it is async.</param>
        /// <returns></returns>
        bool Add<T>(string key, T value, int expiresInMinutes, out Task task);

        /// <summary>
        /// Sets the specified value withkey.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="expiresInMinutes">The expires in.</param>
        /// <param name="task">The task if it is async.</param>
        /// <returns></returns>
        bool Set<T>(string key, T value, int expiresInMinutes, out Task task);

        /// <summary>
        /// Flushes all.
        /// </summary>
        void FlushAll();

        /// <summary>
        /// Flushes all.
        /// </summary>
        /// <param name="task">The task.</param>
        void FlushAll(out Task task);

        /// <summary>
        /// Gets or sets the sliding time to live.
        /// When the cache item has this amount of time left, it will be marked for refresh
        /// </summary>
        /// <value>
        /// The sliding time to live.
        /// </value>
        int SlidingTimeToLive { get; set; }

        /// <summary>
        /// Determines whether the cache item is about the expire.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>
        ///   <c>true</c> means you need to refresh the value. <c>false</c> means do nothing, there is still enough time.
        /// </returns>
        bool IsAboutToExpire(string key);

        /// <summary>
        /// Determines whether the specified key is extending by one of the clients.
        /// If the extension is in process, do not try to extend same value
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>
        ///   <c>true</c> if the specified key is extending; otherwise, <c>false</c>.
        /// </returns>
        bool IsExtending(string key);

        /// <summary>
        /// Sets the key as extending so other clients cannot extend it in the meanwhile.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        bool SetKeyAsExtending(string key);
    }
}
