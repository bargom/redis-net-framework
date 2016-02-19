using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using Newtonsoft.Json;
using Ninject;
using ServiceStack.Text;
using Baris.Common.Cache.Clients;
using Baris.Common.Cache.Interfaces;
using Baris.Common.Configuration;
using Baris.Common.Helper;

namespace Baris.Common.Cache
{
    /// <summary>
    /// Manages the configured cache classes and cache objects
    /// </summary>
    public sealed class CacheManager : ICacheManager
    {
        #region Members & Constructor

        //configuration keys
        private readonly int _expireInMinutesFirstCache;
        private readonly int _expireInMinutesSecondCache; 

        private readonly ICacheClient _firstCacheClient;
        private readonly ICacheClient _secondCacheClient;
        private readonly bool _isEnabled;

        private static readonly ILog Log = LogManager.GetLogger(typeof(ICacheManager));

        public CacheManager(ICacheClient firstCacheClient, ICacheClient secondCacheClient, ICacheManagerConfiguration cacheManagerConfiguration)
        {
            Argument.NotNull(firstCacheClient, "firstCacheClient"); //second one can be null
            Argument.NotNull(cacheManagerConfiguration, "cacheManagerConfiguration");

            _firstCacheClient = firstCacheClient;
            _secondCacheClient = secondCacheClient;

            //configuration is setup here, because during injection we dont know first or second object, but only one type registration
            _firstCacheClient.SlidingTimeToLive = cacheManagerConfiguration.Caches.FirstCacheClient.SlidingTimeToLive;
            _expireInMinutesFirstCache = cacheManagerConfiguration.Caches.FirstCacheClient.ExpiresInMinutes;
            _secondCacheClient.SlidingTimeToLive = cacheManagerConfiguration.Caches.SecondCacheClient.SlidingTimeToLive;
            _expireInMinutesSecondCache = cacheManagerConfiguration.Caches.SecondCacheClient.ExpiresInMinutes;
            _isEnabled = cacheManagerConfiguration.Enabled;

            //Configure function of DateTime to use different json parser - ServiceStack.Redis.Text cannot handle Unspecified DateTime
            JsConfig<DateTime>.SerializeFn = d => JsonConvert.SerializeObject(d);
            JsConfig<DateTime>.DeSerializeFn = s => JsonConvert.DeserializeObject<DateTime>(s.Replace("\\\"", ""));
            JsConfig<DateTime?>.SerializeFn = d => JsonConvert.SerializeObject(d);
            JsConfig<DateTime?>.DeSerializeFn = s => JsonConvert.DeserializeObject<DateTime?>(s.Replace("\\\"", ""));
        }

        #endregion

        #region Properties

        public ICacheClient CacheClient
        {
            get
            {
                return _firstCacheClient;
            }
        }

        public ICacheClient SecondCacheClient
        {
            get
            {
                return _secondCacheClient;
            }
        }

        public bool IsEnabled
        {
            get
            {
                return _isEnabled;
            }
        }

        #endregion

        #region Singleton

        //SINGLETON COMMENTED BECAUSE OF IOC and MOVED TO NINJECT

        ////.net framework will handle the initialization order, no need to appy lock scenario of singleton pattern
        ////refer to http://msdn.microsoft.com/en-us/library/ff650316.aspx
        //private static readonly CacheManager _instance = new CacheManager();

        ////the class cannot be created outside
        //private CacheManager() { }

        //// Explicit static constructor to tell C# compiler not to mark type as beforefieldinit
        //// stops: type initializer invoked at any time before the first reference to a static field in it
        ////http://csharpindepth.com/Articles/General/BeforeFieldInit.aspx
        //static CacheManager()
        //{
        //}

        ////only possible way to access
        //public static CacheManager Instance
        //{
        //    get 
        //    {
        //        //return the initialized singleton object of cache manager
        //        return _instance; 
        //    }
        //}

        #endregion

        #region TryCache functions

        private static T TryCacheInternal<T>(ICacheClient cacheClient, Func<T> method, string key, int expireInMinutes, bool cacheNull = true)
        {
            //first try to retrieve value
            T value;
            Log.Debug("CacheManager.TryCacheInternal<T>.Start Reading value, key:" + key);
            if (cacheClient.Get(key, out value))
            {
                Log.InfoFormat("CacheManager.CACHE.HIT: {0}", key);
                if (cacheClient.IsAboutToExpire(key))
                {
                    //re-read the value async while returning the old value
                    Log.InfoFormat("CacheManager.CACHE.EXTEND: {0}", key);
                    Task.Run(() => ExtendCache(cacheClient, method, key, expireInMinutes));
                }
                return value;
            }

            //value not found so get value
            Log.InfoFormat("CacheManager.CACHE.MISS: {0}, Run method: {1}", key, method.Method.Name);
            value = method();

            //store it into cache & return
            if (!Equals(value, null) || cacheNull)    //if value is not null, cache it OR even if it is null, but cacheNull value is true, than cache it anyway
            {
                cacheClient.Set(key, value, expireInMinutes);
            }

            return value;
        }

        public T TryCache<T>(Func<T> method, string key, bool cacheNull = true)
        {
            return TryCacheInternal(_firstCacheClient, method, key, _expireInMinutesFirstCache, cacheNull);
        }

        public T TryCache<T>(Func<T> method, string key, int expireInMinutes, bool cacheNull = true)
        {
            return TryCacheInternal(_firstCacheClient, method, key, expireInMinutes, cacheNull);
        }

        public T TryCacheToSecondClient<T>(Func<T> method, string key, bool cacheNull = true)
        {
            if (_secondCacheClient is NoneCacheClient)
            {
                return TryCache(method, key);
            }

            return TryCacheInternal(_secondCacheClient, method, key, _expireInMinutesSecondCache, cacheNull);
        }

        public T TryCacheToSecondClient<T>(Func<T> method, string key, int expireInMinutes, bool cacheNull = true)
        {
            if (_secondCacheClient is NoneCacheClient)
            {
                return TryCache(method, key, expireInMinutes);
            }

            return TryCacheInternal(_secondCacheClient, method, key, expireInMinutes, cacheNull);
        }

        #endregion

        #region FromCache Functions

        public bool FromCache<T>(string key, out T convertedValue)
        {
            return _firstCacheClient.Get(key, out convertedValue);
        }

        public bool FromSecondCache<T>(string key, out T convertedValue)
        {
            if (_secondCacheClient is NoneCacheClient)
            {
                return FromCache(key, out convertedValue);
            }

            return _secondCacheClient.Get(key, out convertedValue);
        }

        #endregion

        #region Extend Cache

        private static void ExtendCache<T>(ICacheClient cacheClient, Func<T> method, string key, int expireInMinutes)
        {
            //check first if some other client is already extending the cache
            if (cacheClient.IsExtending(key))
                return;

            Log.Debug("CacheManager.ExtendCache<T>.extending value of key:" + key);

            //set as extending, so other clients will not try
            cacheClient.SetKeyAsExtending(key);

            //run method
            var value = method();

            //set value (it should be existing, so we set it)
            cacheClient.Set(key, value, expireInMinutes);
        }

        #endregion

        #region To Cache Functions

        public bool ToCache<T>(string key, T value, int expireInMinutes)
        {
            return _firstCacheClient.Set(key, value, expireInMinutes);
        }

        public bool ToCache<T>(string key, T value)
        {
            return _firstCacheClient.Set(key, value);
        }

        public bool ToSecondCache<T>(string key, T value, int expireInMinutes)
        {
            if (_secondCacheClient is NoneCacheClient)
            {
                return ToCache(key, value, expireInMinutes);
            }

            return _secondCacheClient.Set(key, value, expireInMinutes);
        }

        public bool ToSecondCache<T>(string key, T value)
        {
            if (_secondCacheClient is NoneCacheClient)
            {
                return ToCache(key, value);
            }

            return _secondCacheClient.Set(key, value);
        }

        #endregion
    }
}
