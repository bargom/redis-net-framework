using System;
using System.Diagnostics;
using System.Threading.Tasks;
using log4net;
using Baris.Common.Cache.Interfaces;
using Baris.Common.Helper;

namespace Baris.Common.Cache.Clients
{
    public abstract class BaseCacheClient : ICacheClient
    {
        #region Members & Constructor

        public const string CacheKeyIsExtendingSuffix = "_$@{EXT}@$";

        protected static readonly ILog _log = LogManager.GetLogger(typeof(ICacheClient));
        protected const int DefaultCacheMinutes = 5; //if nothing is setup, this is used
        public int SlidingTimeToLive { get; set; }

        protected BaseCacheClient()
        {
            SlidingTimeToLive = 0;
        }

        #endregion

        #region Logging

        protected Stopwatch LogStart(string message)
        {
            _log.Info("Start." + message);
            return Stopwatch.StartNew();
        }

        protected void LogStop(string message, Stopwatch stopwatch)
        {
            stopwatch.Stop();
            _log.Info("Stop." + message + ", Elapsed time (ms): " + stopwatch.ElapsedMilliseconds);
        }

        #endregion

        #region Shared Methods

        public virtual T Get<T>(string key)
        {
            T value;
            var retrieved = Get(key, out value);
            return retrieved ? value : default(T);
        }

        public virtual bool Add<T>(string key, T value)
        {
            return Add(key, value, DefaultCacheMinutes);
        }

        public virtual bool Set<T>(string key, T value)
        {
            return Set(key, value, DefaultCacheMinutes);
        }

        public virtual bool Add<T>(string key, T value, out Task task)
        {
            return Add(key, value, DefaultCacheMinutes, out task);
        }

        public virtual bool Set<T>(string key, T value, out Task task)
        {
            return Set(key, value, DefaultCacheMinutes, out task);
        }

        #endregion

        #region Abstract Methods

        public abstract bool Exists(string key);

        public abstract bool Get<T>(string key, out T value);

        public abstract bool Add<T>(string key, T value, int expiresInMinutes);

        public abstract bool Set<T>(string key, T value, int expiresInMinutes);

        public abstract void FlushAll();

        public abstract bool IsAboutToExpire(string key);

        #endregion

        #region Dispose related

        protected bool _isDisposed = false;

        public void Dispose() 
        {
            Dispose(true);
            GC.SuppressFinalize(this); 
        }

        protected virtual void Dispose(bool notCallingFromFinalize) 
        {
            if (_isDisposed)
                return;

            if (notCallingFromFinalize) 
            {
                // free managed
                
            }
            // free unmanaged
            //...

            //set as disposed
            _isDisposed = true;
        }

        ~BaseCacheClient()
        {
            // Simply call Dispose(false).
            Dispose (false);
        }

        #endregion

        #region Methods without async calls

        public virtual bool Add<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            //if not overwritten, no async call
            task = null;
            return Add(key, value, expiresInMinutes);
        }

        public virtual bool Set<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            //if not overwritten, no async call
            task = null;
            return Set(key, value, expiresInMinutes);
        }

        public virtual void FlushAll(out Task task)
        {
            //if not overwritten, no async call
            task = null;
            FlushAll();
        }

        #endregion

        #region Helpers for derived classes

        protected static bool SafeConvert<T>(object value, T defaultValue, out T convertedValue)
        {
            if (value == DBNull.Value)
            {
                convertedValue = defaultValue;
                return false;
            }

            try
            {
                if (value == null)
                    convertedValue = defaultValue;
                else
                    convertedValue = (T) value;
            }
            catch
            {
                convertedValue = defaultValue;
                if (value != null)
                {
                    _log.ErrorFormat("Cannot convert memory object from type:{0} to type:{1}",
                        value.GetType().Name, typeof (T).Name);
                }
            }

            return true;
        }

        #endregion

        #region Sliding Cache

        public bool IsExtending(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            //check the key marker that show it is in extension process
            var cacheExtendingKeyExists = Exists(key + CacheKeyIsExtendingSuffix);
            _log.DebugFormat("Checking extension key:{0}{1}, resulted:{2}", key, CacheKeyIsExtendingSuffix, cacheExtendingKeyExists);

            return cacheExtendingKeyExists;
        }
         
        public bool SetKeyAsExtending(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            //it will expire after sliding time to live, so next request can extend if not finished still
            _log.DebugFormat("Adding extension key:{0}{1} for the period of:{2} minutes", key, CacheKeyIsExtendingSuffix, SlidingTimeToLive);
            var ok = Set(key + CacheKeyIsExtendingSuffix, 1, SlidingTimeToLive); 

            return ok;
        }

        #endregion

    }
}
