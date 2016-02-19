using System;
using System.Collections.Generic;
using System.Drawing.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.CloudWatch.Model;
using ServiceStack.Redis;
using Baris.Common.Cache.Interfaces;
using Baris.Common.Cache.RedisNative;
using Baris.Common.Cache.Server;
using Baris.Common.Helper;

namespace Baris.Common.Cache.Clients
{
    /// <summary>
    /// This class will be used to connect to redis slave and master to read/write operation to cache.
    /// It uses ServiceStack RedisClient for the connection.
    /// Read operations and write are different, read is sync, write is async.
    /// </summary>
    public class RedisCacheClient : BaseCacheClient
    {
        #region Thread safe redis clients

        public IRedisClientNullable CreateWriterRedisClient()
        {
            //for thread safe, we need new instance otherwise socket connections are called at the same time
            //var writer = (IRedisClientNullable)_redisClientFactory.CreateRedisClient(_writerRedisHost, _writerRedisPort);
            var writer = (IRedisClientNullable)_pooledRedisClientManager.GetClient(); //this method uses writer collection, just another collection
            return writer;
        }

        public IRedisClientNullable CreateReaderRedisClient()
        {
            //for thread safe, we need new instance otherwise socket connections are called at the same time
            //var client = (IRedisClientNullable)_redisClientFactory.CreateRedisClient(_readerRedisHost, _readerRedisPort);
            var client = (IRedisClientNullable)_pooledRedisClientManager.GetReadOnlyClient(); //using slaves (readonly) collection. although for our use, it is not read only
            return client;
        }

        public IRedisClientNullable CreateReaderRedisClientNotPooled() //to be used before the pool is created
        {
            //for thread safe, we need new instance otherwise socket connections are called at the same time
            var client = (IRedisClientNullable)_redisClientFactory.CreateRedisClient(_readerRedisHost, _readerRedisPort);
            return client;
        }

        public string GetReaderRedisClientAddress()
        {
            return string.Format("{0}:{1}", _readerRedisHost, _readerRedisPort);
        }

        public string GetWriterRedisClientAddress()
        {
            return string.Format("{0}:{1}", _writerRedisHost, _writerRedisPort);
        }

        public string GetWriterRedisClientAddressHost()
        {
            return _writerRedisHost;
        }

        public int GetWriterRedisClientAddressPort()
        {
            return _writerRedisPort;
        }

        #endregion

        #region Members & Constructor & Dispose

        //private readonly NullObject _nullValueIndicator = new NullObject();
        private readonly DBNull _nullValueIndicator = DBNull.Value;
        //private readonly IRedisClientFactory _redisClientFactory;
        private readonly IRedisCacheServer _cacheServer;

        private string _writerRedisHost;
        private int _writerRedisPort;
        private readonly string _readerRedisHost;
        private readonly int _readerRedisPort;
        private PooledRedisClientManager _pooledRedisClientManager;
        private readonly IRedisClientFactory _redisClientFactory;

        /// <summary>
        /// Instead of ICacheManagerConfiguration, we are setting individual values, because later on we can change this values
        /// to set the new redis writer after failover.
        /// </summary>
        /// <param name="redisClientFactory"></param>
        /// <param name="cacheServer"></param>
        /// <param name="writerRedisHost"></param>
        /// <param name="writerRedisPort"></param>
        /// <param name="readerRedisHost"></param>
        /// <param name="readerRedisPort"></param>
        public RedisCacheClient(IRedisClientFactory redisClientFactory, IRedisCacheServer cacheServer,
            string writerRedisHost, int writerRedisPort, string readerRedisHost, int readerRedisPort)
        {
            Argument.NotNull(redisClientFactory, "redisClientFactory");
            Argument.NotNull(cacheServer, "cacheServer");
            Argument.NotNullOrEmpty(writerRedisHost, "writerRedisHost");
            Argument.NotNegativeOrZero(writerRedisPort, "writerRedisPort");
            Argument.NotNullOrEmpty(readerRedisHost, "readerRedisHost");
            Argument.NotNegativeOrZero(readerRedisPort, "readerRedisPort");

            _redisClientFactory = redisClientFactory;
            _cacheServer = cacheServer;
            _writerRedisHost = writerRedisHost;
            _writerRedisPort = writerRedisPort;
            _readerRedisHost = readerRedisHost;
            _readerRedisPort = readerRedisPort;

            //check slave info to see if it is already connected to other slave
            //it is possible that while initializing new instance, in database the master can be different
            //than the master in web.config. This is because maybe the master is changed after a failover.
            //so we will read the master from current slave, without going to database
            //we can do it sync, so in singleton, it is for sure that it is called once. (singleton via ninject)
            cacheServer.SetMasterInAppDomainLoad(this, writerRedisHost, writerRedisPort);

            //to support pooling, we add this last step, becuase cacheServer.SetMasterInAppDomainLoad can change the _writerRedisHost to new address
            //from database
            CreatePooledRedisClientManager();
        }

        private void CreatePooledRedisClientManager()
        {
            _pooledRedisClientManager = new PooledRedisClientManager(
                new List<string>() { GetWriterRedisClientAddress() }, //we define only one master 
                new List<string>() { GetReaderRedisClientAddress() }, //and we have only one slave, which is the local one, so no load balancing but only pooling
                new RedisClientManagerConfig()
                {
                    AutoStart = false, //starts the pool when this class is initialized
                    MaxReadPoolSize = 1000, //pool size for slave connections (to localhost)
                    MaxWritePoolSize = 1000 //pool size for master connections (to remote server)
                });

            //after creating change client factory to our nullable creator
            _pooledRedisClientManager.RedisClientFactory = _redisClientFactory;

            //now ready to start the pool
            _pooledRedisClientManager.Start();
        }

        protected override void Dispose(bool notCallingFromFinalize)
        {
            //if not disposed before 
            if (_isDisposed)
                return;

            //if it is called from Dispose() method
            if (notCallingFromFinalize)
            {
                _pooledRedisClientManager.Dispose();
            }
            _isDisposed = true;
        }

        #endregion

        #region Switch Writer values to new master

        /// <summary>
        /// Switches the writer config to new master.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <param name="port">The port.</param>
        public void SwitchWriterConfigToNewMaster(string host, int port)
        {
            Argument.NotNullOrEmpty(host, "host");
            Argument.NotNegativeOrZero(port, "port");

            _log.InfoFormat("RedisCacheClient.SwitchWriterToNewMaster.Changing the writer redis client to host:{0}, port:{1}", host, port);
            _writerRedisHost = host;
            _writerRedisPort = port;

            //only if it is already defined, we need to update with new values
            if (_pooledRedisClientManager != null)
            {
                _log.InfoFormat("RedisCacheClient.SwitchWriterToNewMaster._pooledRedisClientManager is not null, re-assign with host:{0}, port:{1}", host, port);

                //CreatePooledRedisClientManager();
                _pooledRedisClientManager.FailoverTo(
                    new List<string>() { GetWriterRedisClientAddress() }, 
                    new List<string>() { GetReaderRedisClientAddress() });
            }
        }

        #endregion

        #region Exists

        public override bool Exists(string key)
        {
            Argument.NotNullOrEmpty(key, "key");

            try
            {
                using (var reader = CreateReaderRedisClient())
                {
                    return reader.ContainsKey(key);
                }
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
            var watch = LogStart("RedisCacheClient.Get<T>." + key);

            try
            {
                using (var reader = CreateReaderRedisClient())
                {
                    var valueFound = reader.Get(key, out value);
                    LogStop("RedisCacheClient.Get<T>." + key + ", Value found:" + valueFound, watch);
                    return valueFound;
                }
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("Cannot retrieve value for key:{0}, type:{1}", key, typeof (T).Name), ex);
                value = default(T);
                return false;
            }
        }

        #endregion

        #region Set/Add methods, sync functions also calling async, so for redis cache, write operations always async

        public override bool Set<T>(string key, T value, int expiresInMinutes)
        {
            Task task;
            return Set(key, value, expiresInMinutes, out task);
        }

        public override bool Add<T>(string key, T value, int expiresInMinutes)
        {
            Task task;
            return Add(key, value, expiresInMinutes, out task);
        }

        #endregion

        #region Set/Add internal methods

        private bool SetWithRedisClient<T>(IRedisClientNullable readerOrWriter, string key, T value, int expiresInMinutes, string typeOfRedis)
        {
            //string and other ref types
            if (Equals(value, null)) //value types already ignored, but nullable types not, so no need to check value type
            {
                //send special value to storage so that we can later convert it back to null
                _log.InfoFormat("Setting key-value to writer Redis ({0}) with special null value. key: {1}, expiration:{2}", typeOfRedis, key, expiresInMinutes);
                return readerOrWriter.SetAsNull(key, new TimeSpan(0, expiresInMinutes, 0));
            }
            _log.InfoFormat("Setting key-value to writer Redis ({0}). key: {1}, expiration:{2}", typeOfRedis, key, expiresInMinutes);
            return readerOrWriter.Set(key, value, new TimeSpan(0, expiresInMinutes, 0));
        }

        private bool AddWithRedisClient<T>(IRedisClientNullable readerOrWriter, string key, T value, int expiresInMinutes, string typeOfRedis)
        {
            //string and other ref types
            if (Equals(value, null)) //value types already ignored, but nullable types not, so no need to check value type
            {
                //send special value to storage so that we can later convert it back to null
                _log.InfoFormat("Adding key-value to writer Redis ({0}) with special null value. key: {1}, expiration:{2}", typeOfRedis, key, expiresInMinutes);
                return readerOrWriter.AddAsNull(key, new TimeSpan(0, expiresInMinutes, 0));
            }
            _log.InfoFormat("Adding key-value to writer Redis ({0}). key: {1}, expiration:{2}", typeOfRedis, key, expiresInMinutes);
            return readerOrWriter.Add(key, value, new TimeSpan(0, expiresInMinutes, 0));
        }

        #endregion

        #region Set/Add methods Async

        public override bool Set<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");

            task = Task.Run(
                () => RunWithFailOver(
                    method: () => 
                    {
                        using (var writer = CreateWriterRedisClient())
                        {
                            return SetWithRedisClient(writer, key, value, expiresInMinutes, "RemoteWriter");
                        } 
                    },
                    info: GetWriterRedisClientAddress() + " << " + key,
                    errorMessage: string.Format("Cannot Set with key:{0}", key))
                );

            //We also set/add the keys to the local, so while waiting the SYNC, the value will be already in for the next call.
            //if the code is making continues calls to methods, the sync timing is not enough for the code.
            //when sync happens, the value will be overwritten with same value, so not harmful.
            //Also failover not required for local
            //And it is a sync call, to be sure it is filled in before getting value.
            RunWithoutFailOver(
                    method: () =>
                    {
                        using (var reader = CreateReaderRedisClient())
                        {
                            return SetWithRedisClient(reader, key, value, expiresInMinutes, "LocalReader");
                        }
                    },
                    info: GetReaderRedisClientAddress() + " << " + key 
                );

            return true;
        }

        public override bool Add<T>(string key, T value, int expiresInMinutes, out Task task)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");

            task = Task.Run(
                () => RunWithFailOver(
                    method: () =>
                    {
                        using (var writer = CreateWriterRedisClient())
                        {
                            return AddWithRedisClient(writer, key, value, expiresInMinutes, "RemoteWriter");
                        }
                    },
                    info: GetWriterRedisClientAddress() + " << " + key,
                    errorMessage: string.Format("Cannot Add with key:{0}", key))
                );

            //We also set/add the keys to the local, so while waiting the SYNC, the value will be already in for the next call.
            //if the code is making continues calls to methods, the sync timing is not enough for the code.
            //when sync happens, the value will be overwritten with same value, so not harmful.
            //Also failover not required for local
            //And it is a sync call, to be sure it is filled in before getting value.
            RunWithoutFailOver(
                method: () =>
                {
                    using (var reader = CreateReaderRedisClient())
                    {
                        return AddWithRedisClient(reader, key, value, expiresInMinutes, "LocalReader");
                    }
                },
                info: GetReaderRedisClientAddress() + " << " + key 
            );

            return true;
        }

        #endregion

        #region Safe methods

        private bool RunWithoutFailOver(Func<bool> method, string info)
        {
            try
            {
                _log.InfoFormat("RedisCacheClient.RunWithoutFailOver: {0} for key {1}", method.Method.Name, info);
                return RunWithoutErrorHandling(method, info);
            }
            catch (Exception ex)
            {
                //no failover
                _log.Error("RedisCacheClient.RunWithoutFailOver failed for key:" + info, ex);
            }
            return false;
        }

        private bool RunWithFailOver(Func<bool> method, string info, string errorMessage)
        {
            try
            {
                _log.InfoFormat("RedisCacheClient.RunWithFailOver: {0} for key {1}", method.Method.Name, info);
                return RunWithoutErrorHandling(method, info);
            }
            catch (Exception ex)
            {
                //second try internally, to avoid all threads start the failover, or the thread can fail while switching the redis master
                //it can try once more to be sure 
                try
                {
                    _log.Error(string.Format("Failed: {0}, Error message: {1}", method.Method.Name, errorMessage), ex);
                    _log.InfoFormat("Trying again 2nd time: {0}", method.Method.Name);

                    //wait for a while to not overload the tcp communication
                    Thread.Sleep(100);

                    return RunWithoutErrorHandling(method, info);
                }
                catch (Exception ex2)
                {
                    _log.Error(string.Format("2nd time Failed: {0}, Error message: {1}", method.Method.Name, errorMessage), ex2);
                }

                //failover is necessary, only one thread can start it.
                _log.InfoFormat("RedisCacheClient.RunWithFailOver.FailOverIfRequiredAsync called for key: {0}", info);
                _cacheServer.FailOverIfRequiredAsync(this, info); //send itself, so that the writer config can be changed
            }
            return false;
        }

        private bool RunWithoutErrorHandling(Func<bool> method, string info)
        {
            var watch = LogStart("RedisCacheClient.RunWithFailOverWithoutErrorHandling." + info);
            var success = method();
            LogStop("RedisCacheClient.RunWithFailOverWithoutErrorHandling." + info, watch);
            return success;
        }

        #endregion

        #region Multi key operations

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
                        using (var writer = CreateWriterRedisClient())
                        {
                            writer.FlushAll();
                            return true;
                        }
                    },
                    info: "None(flushing)",
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

            using (var reader = CreateReaderRedisClient())
            {
                var ttl = reader.GetTimeToLive(key).TotalMinutes;

                if (ttl < 0)
                    return false;
                        //it is already removed, no need to go into sliding logic, it should be handled next cache hit

                return (ttl <= SlidingTimeToLive);
            }
        }

        #endregion
    }
}
