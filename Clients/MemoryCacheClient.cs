using System;
using System.Linq;
using System.Runtime.Caching;
using Baris.Common.Helper;

namespace Baris.Common.Cache.Clients
{
    /// <summary>
    /// This class will be used to read/write to in memory cache.
    /// Nothing is async, and not supporting any kind of clustering but only local memory is used.
    /// </summary>
    public class MemoryCacheClient : BaseCacheClient
    {
        #region Properties 

        public const string TimeToLiveKeySuffix = "_$@{TTL}@$";
        public const string NullValueIndicator = "${<MEMORYNULLOBJECT>}$";

        #endregion

        #region Exists

        public override bool Exists(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            try
            {
                return MemoryCache.Default.Contains(key);
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("Cannot check existance key:{0}", key), ex);
                return false;
            }
        }

        #endregion

        #region Get methods

        public override bool Get<T>(string key, out T value)
        {
            Argument.NotNullOrEmpty(key, "key");
            var watch = LogStart("MemoryCacheClient.Get<T>." + key);

            try
            {
                //read from memory cache
                var valueInMemory = MemoryCache.Default.Get(key);

                //if null, does not exist
                if (valueInMemory == null)
                {
                    value = default(T);
                    LogStop("MemoryCacheClient.Get<T>." + key, watch);
                    return false;
                }

                if (valueInMemory.ToString() == NullValueIndicator)
                    valueInMemory = null;
                
                //convert and return the convertion result as success of function
                var success = SafeConvert(valueInMemory, default(T), out value);
                LogStop("MemoryCacheClient.Get<T>." + key, watch);
                return success;
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("Cannot retrieve value for key:{0}, type:{1}", key, typeof(T).Name), ex);
                value = default(T);
                return false;
            }
        }

        #endregion

        #region Set/Add methods, sync functions also calling async, so for redis cache, write operations always async

        public override bool Set<T>(string key, T value, int expiresInMinutes)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");
            var watch = LogStart("MemoryCacheClient.Set<T>." + key);

            //this will overwrite if exists
            var whenToExpire = DateTime.Now.AddMinutes(expiresInMinutes);
            if (Equals(value, null))
                MemoryCache.Default.Set(key, NullValueIndicator, whenToExpire);
            else
                MemoryCache.Default.Set(key, value, whenToExpire);

            //set also expiration tracker, it will expire x minutes before the real value, so it tracks the main cache needs refresh
            if (SlidingTimeToLive>0)
                MemoryCache.Default.Set(key + TimeToLiveKeySuffix, 1, whenToExpire.Subtract(new TimeSpan(0, SlidingTimeToLive, 0)));

            LogStop("MemoryCacheClient.Set<T>." + key, watch);
            return true;
        }

        public override bool Add<T>(string key, T value, int expiresInMinutes)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");
            var watch = LogStart("MemoryCacheClient.Add<T>." + key);

            //this will insert new and if key exists it will return false
            var whenToExpire = DateTime.Now.AddMinutes(expiresInMinutes);
            bool added = MemoryCache.Default.Add(key, value, whenToExpire);

            if (SlidingTimeToLive>0 && added)
            {
                //set also expiration tracker, it will expire x minutes before the real value, so it tracks the main cache needs refresh
                MemoryCache.Default.Add(key + TimeToLiveKeySuffix, 1, whenToExpire.Subtract(new TimeSpan(0, SlidingTimeToLive, 0)));
            }

            LogStop("MemoryCacheClient.Add<T>." + key, watch);
            return added;
        }

        #endregion

        #region Multi key operations

        public override void FlushAll()
        {
            //not recommended but at the moment best case!!
            foreach (var cacheKey in MemoryCache.Default.Select(kvp => kvp.Key).ToList())
                MemoryCache.Default.Remove(cacheKey);
        }
        
        #endregion

        #region Sliding cache

        public override bool IsAboutToExpire(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            if (SlidingTimeToLive <= 0)
                return false;

            int value;
            var cacheTrackerExists = MemoryCache.Default.Contains(key + TimeToLiveKeySuffix);

            //cache tracker is expired, [_slidingTimeToLive] minutes left for the main object to expire.
            return !cacheTrackerExists;
        }

        #endregion
    }
}
