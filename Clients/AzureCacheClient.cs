using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.ApplicationServer.Caching;
using Baris.Common.Helper;
using Baris.Common.Logging;

namespace Baris.Common.Cache.Clients
{
    /// <summary>
    /// This class will be used to connect to Azure distributed cache that is configured.
    /// Read operations and write are different, read is sync, write is async.
    /// </summary>
    public class AzureCacheClient : BaseCacheClient
    {
        #region Properties & Constructor & Dispose

        protected DataCacheFactory AzureDataCacheFactory { get; set; } //it should be live as long as the datacache is live
        protected DataCache AzureDataCache { get; set; }

        public AzureCacheClient() : this(false){}

        public AzureCacheClient(int timeOutSeconds)
        {
            Initialize(timeOutSeconds);
        }

        public AzureCacheClient(bool skipInitialize)
        {
            //only use TRUE for unit testing, otherwise if will fail when methods called.
            if (!skipInitialize)
                Initialize();
        }

        private void Initialize(int? timeOutSeconds = null)
        {
            try
            {
                _log.Info("AzureCacheClient.Initialize: Start initialize AzureCacheClient.");
                if (AzureDataCache != null)
                {
                    _log.Info("AzureCacheClient.Initialize: AzureDataCache is not null, exit initialize method.");
                    return; //no need to configure
                }

                _log.Info("AzureCacheClient.Initialize: Creating DataCacheFactoryConfiguration.");
                var dataCacheFactoryConfig = new DataCacheFactoryConfiguration(); //it will read the "default"
                if (timeOutSeconds.HasValue)
                {
                    _log.InfoFormat("AzureCacheClient.Initialize: timeOutSeconds has value of {0}, setting timeout.", timeOutSeconds.Value);
                    dataCacheFactoryConfig.ChannelOpenTimeout = new TimeSpan(0, 0, timeOutSeconds.Value);
                }

                _log.Info("AzureCacheClient.Initialize: Creating DataCacheFactory.");
                AzureDataCacheFactory = new DataCacheFactory(dataCacheFactoryConfig);
                 
                _log.Info("AzureCacheClient.Initialize: Creating AzureDataCache with default value.");
                AzureDataCache = AzureDataCacheFactory.GetDefaultCache();
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorsException(
                    "AzureCacheClient.Initialize: Failed to initialize: Cannot find default configuration or no servers defined for DataCacheFactoryConfiguration, please add <dataCacheClients> section to configuration.", ex);
            }
        }

        protected override void Dispose(bool notCallingFromFinalize)
        {
            //if not disposed before 
            if (_isDisposed) 
                return;

            //if it is called from Dispose() method
            if (notCallingFromFinalize)
            {
                if (AzureDataCacheFactory != null)
                    AzureDataCacheFactory.Dispose();

                AzureDataCacheFactory = null;
                AzureDataCache = null; 
            }
            _isDisposed = true;
        }

        #endregion

        #region Exists

        public override bool Exists(string key)
        {
            Argument.NotNullOrEmpty(key, "key");
            var watch = LogStart("AzureCacheClient.Exists." + key);

            try
            {
                //azure does not provide method to check
                var value = AzureDataCache.Get(key) != null;
                LogStop("AzureCacheClient.Exists." + key, watch);
                return value;
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("AzureCacheClient.Exists: Cannot check existance key:{0}", key), ex);
                return false;
            }
        }

        #endregion

        #region Get methods

        public override bool Get<T>(string key, out T value)
        {
            Argument.NotNullOrEmpty(key, "key");
            var watch = LogStart("AzureCacheClient.Get<T>." + key);

            try
            {
                //read from memory cache
                var valueInMemory = AzureDataCache.Get(key);
                if (valueInMemory == null)
                {
                    value = default(T);
                    LogStop("AzureCacheClient.Get<T>.Nothing found for key: " + key, watch);
                    return false;
                }

                //convert and return the convertion result as success of function
                var success = SafeConvert(valueInMemory, default(T), out value);
                LogStop("AzureCacheClient.Get<T>." + key, watch);
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

        //redirected to async call
        public override bool Set<T>(string key, T value, int expiresInMinutes)
        {
            Task task;
            return Set(key, value, expiresInMinutes, out task);
        }

        //redirected to async call
        public override bool Add<T>(string key, T value, int expiresInMinutes)
        {
            Task task;
            return Add(key, value, expiresInMinutes, out task);
        }

        #endregion

        #region Set/Add methods Async

        public override bool Set<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");

            task = Task.Run(
                () => RunWithFailOver(
                    method: () => AzureDataCache.Put(key, value, new TimeSpan(0, expiresInMinutes, 0)), 
                    key: key,
                    errorMessage: string.Format("Cannot Set with key:{0}", key))
            );
            return true;
        }

        public override bool Add<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");

            task = Task.Run(
                () => RunWithFailOver(
                    method: () => AzureDataCache.Add(key, value, new TimeSpan(0, expiresInMinutes, 0)),
                    key: key,
                    errorMessage: string.Format("Cannot Add with key:{0}", key))
            );
            return true;
        }

        #endregion

        #region Safe methods

        private DataCacheItemVersion RunWithFailOver(Func<DataCacheItemVersion> method, string key, string errorMessage)
        {
            try
            {
                var watch = LogStart("AzureCacheClient.RunWithFailOver." + key + ", method:" + method.Method.Name);
                var version = method();
                LogStop("AzureCacheClient.RunWithFailOver." + key + ", method:" + method.Method.Name, watch);
                return version;
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("Failed method: {0}, Error message: {1}", method.Method.Name, errorMessage), ex);
                //todo: call the failover service for azure (ASYNC) --> FOR FUTURE IF POSSIBLE IN AZURE TO FAILOVER
            }
            return null;
        }

        #endregion

        #region Multi key operations

        //flush all is also redirected to the async call, we dont need to wait for the flush
        public override void FlushAll()
        {
            Task task;
            FlushAll(out task);
        }
        
        public override void FlushAll(out Task task)
        {
            task = Task.Run(
                () => RunWithFailOver(
                    method: () =>
                    {
                        //this call can fail, test it with azure!
                        foreach (var obj in AzureDataCache.GetSystemRegions().SelectMany(AzureDataCache.GetObjectsInRegion))
                        {
                            AzureDataCache.Remove(obj.Key);
                        }
                        return null;
                    },
                    key: "None(flushing)",
                    errorMessage: "Cannot Flush all")
            );
        }

        #endregion

        #region Sliding cache

        public override bool IsAboutToExpire(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            if (SlidingTimeToLive <= 0)
                return false;

            var cacheItem = AzureDataCache.GetCacheItem(key);

            if (cacheItem == null)
                return false; //it is already removed, no need to go into sliding logic, it should be handled next cache hit

            return (cacheItem.Timeout.TotalMinutes <= SlidingTimeToLive);
        }

        #endregion
    }
}
