using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.XPath;
using log4net;
using Microsoft.WindowsAzure.ServiceRuntime;
using ServiceStack.Redis;
using Baris.Common.Cache.Clients;
using Baris.Common.Cache.Interfaces;
using Baris.Common.Cache.RedisNative;
using Baris.Common.Configuration;
using Baris.Common.Configuration.Sections;
using Baris.Common.Helper;

namespace Baris.Common.Cache.Server
{
    /// <summary>
    /// Server related calls should be made over this class. 
    /// At the moment only supporting failover scenario.
    /// Future uses can be to cleanup keys...
    /// ALL Logging is set to WARN, so setting a level of warn will also log anything related to 
    /// important events like changing the master server.
    /// </summary>
    public class RedisCacheServer : IRedisCacheServer
    {
        #region Constructor & Properties


        private const string SelectStatement = "select top 1 ServerHost from RedisMasterDeployment where DeploymentID = @DeploymentId order by InsertedDate desc";
        private const string UpdateStatement = "INSERT INTO RedisMasterDeployment(ServerHost, InsertedBy, Description, DeploymentId) VALUES(@ServerHost, @InsertedBy, @Description, @DeploymentId)";

        private readonly string _redisSlaveHost;
        private readonly int _redisSlavePort;
        private readonly string _redisMasterConnectionString;
        private readonly string _redisIpFile;

        protected static readonly ILog Log = LogManager.GetLogger(typeof(RedisCacheServer));

        public RedisCacheServer(ICacheManagerConfiguration cacheManagerConfiguration)
        {
            Argument.NotNull(cacheManagerConfiguration, "cacheManagerConfiguration");

            _redisSlaveHost = cacheManagerConfiguration.Redis.Slave.Host;
            _redisSlavePort = cacheManagerConfiguration.Redis.Slave.Port;
            _redisMasterConnectionString = cacheManagerConfiguration.Redis.ConnectionString;
            _redisIpFile = cacheManagerConfiguration.Redis.RecoverToMasterIpFromFile;
        }

        public RedisCacheServer(string redisSlaveHost, int redisSlavePort, string redisMasterConnectionString, string redisIpFile)
        {
            Argument.NotNullOrEmpty(redisSlaveHost, "redisSlaveHost");
            Argument.NotNegativeOrZero(redisSlavePort, "redisSlavePort");
            Argument.NotNullOrEmpty(redisMasterConnectionString, "redisMasterConnectionString");

            _redisSlaveHost = redisSlaveHost;
            _redisSlavePort = redisSlavePort;
            _redisMasterConnectionString = redisMasterConnectionString;
            _redisIpFile = redisIpFile;
        }

        #endregion

        #region Initial WebRole Master selection

        private static void LogToWebRole(string text)
        {
            try
            {
                File.AppendAllText("c:\\Logs\\Webrole.txt", "WebRole.OnStart (RedisCacheServer.TestMasterRedisAndChangeIfRequired): " + text + "\r\n");
            }
            catch
            {
                //nothing logged
            }
        }

        /// <summary>
        /// Only call from WebRole.OnStart()
        /// </summary>
        public static void TestMasterRedisAndChangeIfRequired()
        {
            try
            {
                string redisConnectionStringForMaster;
                string redisSlaveHost;
                int redisSlavePort;
                string redisMasterHost;
                int redisMasterPort;
                string redisRecoverFromIp;

                if (!GetValuesFromConfig(out redisConnectionStringForMaster, out redisSlaveHost, out redisSlavePort, out redisMasterHost, out redisMasterPort, out redisRecoverFromIp))
                    return;

                //create server related object
                LogToWebRole(string.Format("Creating RedisCacheServer object with config. slavehost:{0}, slaveport:{1}, conn:{2}, ipfile:{3}",
                    redisSlaveHost, redisSlavePort, redisConnectionStringForMaster, redisRecoverFromIp));
                var redisServer = new RedisCacheServer(redisSlaveHost, redisSlavePort, redisConnectionStringForMaster, redisRecoverFromIp);

                LogToWebRole(string.Format("Creating RedisCacheClient object to send key. masterhost:{0}, masterport:{1}", redisMasterHost, redisMasterPort));
                //in this initialization, RedisCacheClient already reads from database and fixes to correct master...
                var currentRedisCacheClient = new RedisCacheClient(new RedisClientNullableFactory(),
                                                                    redisServer,
                                                                    redisMasterHost, redisMasterPort,
                                                                    redisSlaveHost, redisSlavePort);

                //we will try to write a key, if it cannot write, it will start the failover process in any case...
                LogToWebRole("So far no error, starting try writing a key");

                //Generating information and setting to key
                var ip = GetLocalIp();
                var info = GenerateInformation();
                LogToWebRole("Retrieved IP: " + ip);
                LogToWebRole("Retrieved information: " + info);

                //Sync failover, if server is not down, it will pass here quickly with just pinging redis master.
                using (var redisSlave = currentRedisCacheClient.CreateReaderRedisClient() as IRedisNativeClient)
                {
                    //sync call, to handle exceptions
                    redisServer.FailOverIfRequiredInternalAsync(redisSlave, currentRedisCacheClient).GetAwaiter().GetResult(); //sync and dont wraps exceptions
                }

                currentRedisCacheClient.Set("RedisSlave:IP_" + ip, info, 1440); //24 hours expiration
                LogToWebRole("All is OK.");
            }
            catch (Exception ex)
            {
                LogToWebRole("WebRole.OnStart: Error in TestMasterRedisAndChangeIfRequired:" + ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        private static bool GetValuesFromConfig(out string redisConnectionStringForMaster, out string redisSlaveHost,
            out int redisSlavePort, out string redisMasterHost, out int redisMasterPort, out string redisRecoverFromIp)
        {
            redisConnectionStringForMaster = "";
            redisSlaveHost = "";
            redisSlavePort = 0;
            redisMasterHost = "";
            redisMasterPort = 0;
            redisRecoverFromIp = "";

            var appRootDir = Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory);
            LogToWebRole("appRootDir:" + appRootDir);

            LogToWebRole("Start reading web.config as xml");
            var reader = new XPathDocument(appRootDir + "\\web.config");
            var nav = reader.CreateNavigator();
            const string rootNodeString = "//" + CacheManagerConfigurationSection.SectionName + "/";

            var cacheManagerEnabledAttribute =
                nav.SelectSingleNode(rootNodeString + "@" + CacheManagerConfigurationSection.EnabledAttribute);

            //read logManager enabled attribute
            bool cacheManagerEnabled = cacheManagerEnabledAttribute != null && cacheManagerEnabledAttribute.ValueAsBoolean;
            if (!cacheManagerEnabled)
            {
                LogToWebRole("LogManager not enabled!");
                return false;
            }
            LogToWebRole("LogManager is enabled!");

            //read first and second strategies (//Baris/cacheManager/caches/first/@strategy)
            var firstStrategyAttribute =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.CachesElement + "/" +
                                     CacheManagerCacheClientConfigurationElement.FirstCacheClientElement + "/@" +
                                     CacheManagerCacheClientConfigurationElementItem.StrategyElement);
            var secondStrategyAttribute =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.CachesElement + "/" +
                                     CacheManagerCacheClientConfigurationElement.SecondCacheClientElement + "/@" +
                                     CacheManagerCacheClientConfigurationElementItem.StrategyElement);

            //be sure that we are using redis
            LogToWebRole("Looking at strategies");
            if ((firstStrategyAttribute != null && firstStrategyAttribute.Value.EqualsIgnoreCase("redis")) ||
                (secondStrategyAttribute != null && secondStrategyAttribute.Value.EqualsIgnoreCase("redis"))
                )
            {
                LogToWebRole("Redis cache strategy found, continue.");
            }
            else
            {
                LogToWebRole("Redis cache strategy not found, exitting TestMasterRedisAndChangeIfRequired.");
                return false;
            }

            //read connection string and ip filename (//Baris/cacheManager/redis/@connectionString)
            var redisConnectionStringForMasterElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/@" +
                                     CacheManagerRedisConfigurationElement.ConnectionStringElement);
            redisConnectionStringForMaster = redisConnectionStringForMasterElement == null
                ? ""
                : redisConnectionStringForMasterElement.Value;

            var redisSlaveHostElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/" +
                                     CacheManagerRedisConfigurationElement.SlaveElement + "/@" +
                                     CacheManagerRedisConfigurationElementItem.HostElement);
            redisSlaveHost = redisSlaveHostElement == null ? "127.0.0.1" : redisSlaveHostElement.Value;

            var redisSlavePortElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/" +
                                     CacheManagerRedisConfigurationElement.SlaveElement + "/@" +
                                     CacheManagerRedisConfigurationElementItem.PortElement);
            redisSlavePort = redisSlavePortElement == null ? 6379 : redisSlavePortElement.ValueAsInt;

            var redisMasterHostElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/" +
                                     CacheManagerRedisConfigurationElement.MasterElement + "/@" +
                                     CacheManagerRedisConfigurationElementItem.HostElement);
            redisMasterHost = redisMasterHostElement == null ? "db" : redisMasterHostElement.Value;

            var redisMasterPortElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/" +
                                     CacheManagerRedisConfigurationElement.MasterElement + "/@" +
                                     CacheManagerRedisConfigurationElementItem.PortElement);
            redisMasterPort = redisMasterPortElement == null ? 6379 : redisMasterPortElement.ValueAsInt;

            var redisRecoverFromIpElement =
                nav.SelectSingleNode(rootNodeString + CacheManagerConfigurationSection.RedisElement + "/@" +
                                     CacheManagerRedisConfigurationElement.RecoverToMasterIpFromFileElement);
            redisRecoverFromIp = redisRecoverFromIpElement == null ? "c:\\redis\\ip.txt" : redisRecoverFromIpElement.Value;
            return true;
        }

        private static string GenerateInformation()
        {
            /*
             *  WebRole.OnStart: Environment Variable: SystemDrive, Value: D:
                WebRole.OnStart: Environment Variable: ProgramFiles(x86), Value: D:\Program Files (x86)
                WebRole.OnStart: Environment Variable: RdRoleResourcesRootPath, Value: C:\Resources\
                WebRole.OnStart: Environment Variable: ProgramW6432, Value: D:\Program Files
                WebRole.OnStart: Environment Variable: PROCESSOR_IDENTIFIER, Value: AMD64 Family 16 Model 8 Stepping 1, AuthenticAMD
                WebRole.OnStart: Environment Variable: TMP, Value: C:\Resources\temp\22b07a6fab004a738b13e3fe848ef637.Baris.Service.API\RoleTemp
                WebRole.OnStart: Environment Variable: PROCESSOR_ARCHITECTURE, Value: AMD64
                WebRole.OnStart: Environment Variable: PATHEXT, Value: .COM;.EXE;.BAT;.CMD;.VBS;.VBE;.JS;.JSE;.WSF;.WSH;.MSC
                WebRole.OnStart: Environment Variable: APPCMD, Value: D:\Windows\system32\inetsrv\APPCMD.exe
                WebRole.OnStart: Environment Variable: RdRoleConfigRootPath, Value: C:\Config\
                WebRole.OnStart: Environment Variable: PROCESSOR_REVISION, Value: 0801
                WebRole.OnStart: Environment Variable: TEMP, Value: C:\Resources\temp\22b07a6fab004a738b13e3fe848ef637.Baris.Service.API\RoleTemp
                WebRole.OnStart: Environment Variable: USERPROFILE, Value: D:\Windows\system32\config\systemprofile
                WebRole.OnStart: Environment Variable: USERNAME, Value: SYSTEM
                WebRole.OnStart: Environment Variable: SystemRoot, Value: D:\Windows
                WebRole.OnStart: Environment Variable: FP_NO_HOST_CHECK, Value: NO
                WebRole.OnStart: Environment Variable: CommonProgramFiles, Value: D:\Program Files\Common Files
                WebRole.OnStart: Environment Variable: RdRoleId, Value: 22b07a6fab004a738b13e3fe848ef637.Baris.Service.API_IN_1
                WebRole.OnStart: Environment Variable: PUBLIC, Value: D:\Users\Public
                WebRole.OnStart: Environment Variable: WA_CONTAINER_SID, Value: S-1-5-80-2115166559-80284332-1644565568-552499834-58461835
                WebRole.OnStart: Environment Variable: ProgramData, Value: D:\ProgramData
                WebRole.OnStart: Environment Variable: RoleInstanceID, Value: Baris.Service.API_IN_1
                WebRole.OnStart: Environment Variable: @AccountUsername, Value: baris
                WebRole.OnStart: Environment Variable: COMPUTERNAME, Value: RD00155D5758C3
                WebRole.OnStart: Environment Variable: RoleName, Value: Baris.Service.API
                WebRole.OnStart: Environment Variable: @Enabled, Value: true
                WebRole.OnStart: Environment Variable: ALLUSERSPROFILE, Value: D:\ProgramData
                WebRole.OnStart: Environment Variable: CommonProgramW6432, Value: D:\Program Files\Common Files
                WebRole.OnStart: Environment Variable: DiagnosticStore, Value: C:\Resources\directory\22b07a6fab004a738b13e3fe848ef637.Baris.Service.API.DiagnosticStore\
                WebRole.OnStart: Environment Variable: RoleRoot, Value: E:
                WebRole.OnStart: Environment Variable: CommonProgramFiles(x86), Value: D:\Program Files (x86)\Common Files
                WebRole.OnStart: Environment Variable: @ConnectionString, Value: UseDevelopmentStorage=true
                WebRole.OnStart: Environment Variable: windir, Value: D:\Windows
                WebRole.OnStart: Environment Variable: NUMBER_OF_PROCESSORS, Value: 1
                WebRole.OnStart: Environment Variable: OS, Value: Windows_NT
                WebRole.OnStart: Environment Variable: ProgramFiles, Value: D:\Program Files
                WebRole.OnStart: Environment Variable: ComSpec, Value: D:\Windows\system32\cmd.exe
                WebRole.OnStart: Environment Variable: RdRoleRoot, Value: E:\
                WebRole.OnStart: Environment Variable: PSModulePath, Value: D:\Windows\system32\WindowsPowerShell\v1.0\Modules\
                WebRole.OnStart: Environment Variable: __WaRuntimeAgent__, Value: WA-Runtime-d6f32098d567a3553e5dccfffa54aa04
                WebRole.OnStart: Environment Variable: APPDATA, Value: D:\Windows\system32\config\systemprofile\AppData\Roaming
                WebRole.OnStart: Environment Variable: USERDOMAIN, Value: WORKGROUP
                WebRole.OnStart: Environment Variable: PROCESSOR_LEVEL, Value: 16
                WebRole.OnStart: Environment Variable: RdRoleLogRootPath, Value: C:\Resources\
                WebRole.OnStart: Environment Variable: RoleDeploymentID, Value: 22b07a6fab004a738b13e3fe848ef637
                WebRole.OnStart: Environment Variable: LOCALAPPDATA, Value: D:\Windows\system32\config\systemprofile\AppData\Local
                WebRole.OnStart: Environment Variable: @AccountExpiration, Value: 2014-08-21T23:59:59.0000000+02:00
             * */
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("[Key:{0}, Value:{1}]", "RdRoleId", Environment.GetEnvironmentVariable("RdRoleId"));
            sb.AppendFormat("[Key:{0}, Value:{1}]", "RoleInstanceID", Environment.GetEnvironmentVariable("RoleInstanceID"));
            sb.AppendFormat("[Key:{0}, Value:{1}]", "COMPUTERNAME", Environment.GetEnvironmentVariable("COMPUTERNAME"));
            sb.AppendFormat("[Key:{0}, Value:{1}]", "RoleName", Environment.GetEnvironmentVariable("RoleName"));
            sb.AppendFormat("[Key:{0}, Value:{1}]", "RoleDeploymentID", Environment.GetEnvironmentVariable("RoleDeploymentID"));
            sb.AppendFormat("[Key:{0}, Value:{1}]", "DateAdded", DateTime.Now);
            return sb.ToString();
        }

        #endregion

        #region Initial load of site/app domain

        /// <summary>
        /// Checks the slave if already different master assigned, different than the web.config value.
        /// </summary>
        public void SetMasterInAppDomainLoad(RedisCacheClient currentRedisCacheClient, string writerRedisHostFromConfig, int writerRedisPortFromConfig)
        {
            try
            {
                Log.Info("RedisCacheServer.SetMasterInAppDomainLoad called from constructor, reading master from database.");
                var dbRedisMasterAndPort = ReadLatestMasterInDatabase(); //calling the sync method

                if (String.IsNullOrEmpty(dbRedisMasterAndPort))
                {
                    //FailOverIfRequiredAsync(currentRedisCacheClient, "SetMasterInAppDomainLoad").Wait();
                    //return;

                    var myHost = GetCurrentIp();
                    var myPort = _redisSlavePort;

                    //lock database record and insert new one 
                    Log.Warn("RedisCacheServer.SetMasterInAppDomainLoad: Sending the new master to database");
                    InsertMasterInDatabaseAsync(string.Format("{0} {1}", myHost, myPort), "RedisSlave",
                            string.Format("Master is initializing")).GetAwaiter().GetResult();
                    Log.Warn("RedisCacheServer.SetMasterInAppDomainLoad: Writing to database completed");

                    Log.Warn("RedisCacheServer.SetMasterInAppDomainLoad: Setting the current redis cache client to write at the new master (even now we are the master)");
                    //setting to local ip, so now the reader and writer is same
                    currentRedisCacheClient.SwitchWriterConfigToNewMaster(_redisSlaveHost, _redisSlavePort);
                    Log.Warn("RedisCacheServer.SetMasterInAppDomainLoad: Completed setting to the new master.");
                    
                    //now we can read the database again
                    dbRedisMasterAndPort = ReadLatestMasterInDatabase(); //calling the sync method
                }

                Log.InfoFormat("RedisCacheServer.SetMasterInAppDomainLoad called from constructor, master from database is {0}.", dbRedisMasterAndPort);
                var dbRedisMasterAndPortArray = dbRedisMasterAndPort.Split(' ');
                var masterFromDb = dbRedisMasterAndPortArray[0];
                var portFromDb = Convert.ToInt32(dbRedisMasterAndPortArray[1]);

                if (masterFromDb.Equals(writerRedisHostFromConfig, StringComparison.OrdinalIgnoreCase) && portFromDb.Equals(writerRedisPortFromConfig))
                {
                    Log.Info("RedisCacheServer.SetMasterInAppDomainLoad: Slave configuration and current web.config are same, so no need to change.");
                    return;
                }

                Log.Info("RedisCacheServer.SetMasterInAppDomainLoad: Changing the master values with the values read from DB, so config values are old");
                Log.InfoFormat("RedisCacheServer.SetMasterInAppDomainLoad.Changing the writer redis client from host:{0}, port:{1} TO host:{2}, port:{3}",
                    writerRedisHostFromConfig, writerRedisPortFromConfig, masterFromDb, portFromDb);

                using (var slave = currentRedisCacheClient.CreateReaderRedisClientNotPooled() as IRedisNativeClient)
                {
                    SwitchToNewMasterAsync(slave, dbRedisMasterAndPort, currentRedisCacheClient, true).GetAwaiter().GetResult(); //sync forcing, also checks ip
                }
                Log.Info("RedisCacheServer.SetMasterInAppDomainLoad. Completed without errors.");
            }
            catch (Exception ex)
            {
                Log.Error("RedisCacheServer.SetMasterInAppDomainLoad, failed to check new master. ", ex);
                throw;
            }
        }

        #endregion

        #region Static variable to hold value that only one failover starts

        private static bool _isFailoverStarted;
        private static DateTime _whenFailoverStarted;
        private readonly object _locker = new object();

        private bool SetFailoverIndicator(bool isStarted)
        {
            Log.WarnFormat("RedisCacheServer.SetFailoverIndicator: Setting failover indicator to: {0}", isStarted);
            //ensure only one thread can change this value
            lock (_locker)
            {
                //if it is equal, it means, it is already set by another threat, so cannot set it.
                if (_isFailoverStarted == isStarted)
                    return false;

                _isFailoverStarted = isStarted;
            }

            //value changed, so quit lock as soon as possible 
            //these values not critical:
            if (isStarted)
                _whenFailoverStarted = DateTime.Now;

            return true;
        }

        private bool FailoverAlreadyStarted(string failoverStarterKey)
        {
            //if failover already started, nothing to do, just wait until failover complete, and we will not hit here.
            if (_isFailoverStarted)
            {
                Log.Warn("RedisCacheServer.FailoverAlreadyStarted: Failover is already started by another thread, so only checking the timeout of one hour. Key:" + failoverStarterKey);
                //check time out of 1 hour
                if (_whenFailoverStarted.AddHours(1) < DateTime.Now)
                {
                    //failover probably failed or ended with error
                    Log.WarnFormat("RedisCacheServer.FailoverAlreadyStarted: Failover is already started by another thread but it did not ended for one hour, so this thread is taking over the failover. Last failover start was at {0}, Key:{1}", _whenFailoverStarted, failoverStarterKey);
                    SetFailoverIndicator(false);
                }

                //still returning without doing anything, for the next thread to do the work
                return true;
            }

            Log.Warn("RedisCacheServer.FailoverAlreadyStarted: Failover is not started by another thread, so this thread can continue to failover process. Key: " + failoverStarterKey);
            return false;
        }

        #endregion

        #region Failover functions

        public async Task FailOverIfRequiredAsync(RedisCacheClient currentRedisCacheClient, string failoverStarterKey)
        {
            //check if other thread already started
            if (FailoverAlreadyStarted(failoverStarterKey))
                return;

            //immediately set failover as started
            if (!SetFailoverIndicator(true)) //trying to set it to true, if it is already true, cannot set, it means it is already set by another thread in the meantime
            {
                //if we cannot set it to true, it means in the meantime, another thread made it true, so cannot continue.
                Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync:  Cannot set the failover indicator to true, it is already set by another thread.");
                return;
            }

            //create redis client now, to avoid caching of Info from the existing slave
            //creating here also allows lazy loading, so it is created when needed for failover, but not always if we inject it via constructor
            using (var redisSlave = currentRedisCacheClient.CreateReaderRedisClient() as IRedisNativeClient)
            {
                //we dont care about the status of this task, it should be fire and forget style
                //with the use of "await" the the control will return to the caller immediately
                await FailOverIfRequiredInternalAsync(redisSlave, currentRedisCacheClient);
            }
        }

        private async Task FailOverIfRequiredInternalAsync(IRedisNativeClient redisSlave, RedisCacheClient currentRedisCacheClient)
        {
            try
            {
                //1a. try to ping server (async)
                Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Started to ping master server.");
                var canPingTask = CanPingAsync(currentRedisCacheClient);

                //if can ping, return, we use redisSlave in a order-by-order way, it is not thread safe, so cannot use real async.
                if (await canPingTask)
                {
                    Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Ping was success, ending the failover process.");
                    return;
                }

                //1b. at the same time get slave info
                //while connected to slave, get the master host name and port
                Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Started to get redis slave information.");
                var slaveInfoTask = GetSlaveInfoAsync(redisSlave, currentRedisCacheClient);

                //start reading database while parsing info, because if we cannot ping, it is probably down already
                //so no need to block the database reading
                Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Started to read database record to determine the current redis master");
                var cts = new CancellationTokenSource();
                var readLatestMasterTask = ReadLatestMasterInDatabaseAsync(cts.Token);

                //take slave info and check status
                var slaveInfo = await slaveInfoTask;
                if (slaveInfo.MasterLinkStatus == MasterLinkStatus.Up)
                {
                    //cancel database operation, this call should be very rare: ping failed but slave ping not yet!
                    Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Slave information returned that the master is UP, terminate failover.");
                    cts.Cancel(false);
                    return;
                }

                //read master host and port either from slave or if cannot, get from current redis object
                var masterHostAndPortOfSlave = string.IsNullOrWhiteSpace(slaveInfo.MasterHostAndPort)
                    ? slaveInfo.MasterHostAndPort
                    : string.Format("{0} {1}", currentRedisCacheClient.GetWriterRedisClientAddressHost(),
                        currentRedisCacheClient.GetWriterRedisClientAddressPort());

                //slave also says master is down, so start the failover...
                //check db and read latest master
                var masterAndPortInDb = await readLatestMasterTask;
                Log.WarnFormat("RedisCacheServer.FailOverIfRequiredInternalAsync: Database read completed, value is: {0}", masterAndPortInDb);
                if (masterAndPortInDb.Equals(masterHostAndPortOfSlave, StringComparison.OrdinalIgnoreCase))
                {
                    //we can promote to master, because the database record and the configured value of the slave is same
                    //that means, this master is not valid anymore, and none of the other slaves promoted to master, so
                    //we can promote to master
                    Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Database record of master redis is same as current, starting to promote to master.");
                    await PromoteToMaster(slaveInfo, redisSlave, currentRedisCacheClient);
                    Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: End of PromoteToMaster, failover complete.");
                    return;
                }

                //the database record is already different than our existing master,
                //so we just need to switch to this new master.
                Log.WarnFormat(
                    "RedisCacheServer.FailOverIfRequiredInternalAsync: The database record is different, so changing master to the new one:{0}",
                    masterAndPortInDb);
                await SwitchToNewMasterAsync(redisSlave, masterAndPortInDb, currentRedisCacheClient, false); //no need to re-check ip, it is done
                Log.Warn("RedisCacheServer.FailOverIfRequiredInternalAsync: Completed switching to new redis master.");
            }
            catch (Exception ex)
            {
                Log.Error("RedisCacheServer.FailOverIfRequiredInternalAsync: Failed with error.", ex);
            }
            finally
            {
                //so another thread can also start the failover is required...
                SetFailoverIndicator(false);
            }
        }

        #endregion

        #region Redis Slave Functions

        /// <summary>
        /// Determines whether this instance [can ping async] the specified redis slave.
        /// </summary>
        /// <param name="currentRedisCacheClient">The current redis cache client.</param>
        /// <returns></returns>
        private async Task<bool> CanPingAsync(RedisCacheClient currentRedisCacheClient)
        {
            try
            {
                //ping to server
                var pingResult = await Task.Run(() =>
                {
                    using (var redisMaster = (IRedisNativeClient)currentRedisCacheClient.CreateWriterRedisClient())
                    {
                        try
                        {
                            Log.WarnFormat("RedisCacheServer.CanPingAsync: Start pinging master {0}", currentRedisCacheClient.GetWriterRedisClientAddress());
                            return redisMaster.Ping();
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Failed ping to redis master, Error:" + ex.Message, ex);
                            return false;
                        }
                    }
                });
                Log.WarnFormat("RedisCacheServer.CanPingAsync: Ping result:{0}", pingResult);
                return pingResult;
            }
            catch (Exception ex)
            {
                //log exception
                Log.Error("Failed ping to redis master, Error:" + ex.Message, ex);
                return false;
            }
        }

        /// <summary>
        /// Gets the slave info async.
        /// </summary>
        /// <param name="redisSlave">The redis slave.</param>
        /// <param name="currentRedisCacheClient">The current redis cache client.</param>
        /// <returns></returns>
        public async Task<RedisSlaveInfo> GetSlaveInfoAsync(IRedisNativeClient redisSlave, RedisCacheClient currentRedisCacheClient)
        {
            try
            {
                //to sync the last status, we set the slaveof again (causes also sync).
                //this will not fail, but will force to refresh the INFO command status.
                redisSlave.SlaveOf(currentRedisCacheClient.GetWriterRedisClientAddressHost(), currentRedisCacheClient.GetWriterRedisClientAddressPort());

                //get the info, as the slave is new object, it will get the latest
                var slaveInfo = await Task.Run(() => RedisSlaveInfo.Parse(redisSlave.Info));
                Log.WarnFormat("RedisCacheServer.GetSlaveInfoAsync: Slave Information:{0}", slaveInfo);
                return slaveInfo;
            }
            catch (Exception ex)
            {
                //log exception
                Log.Error("RedisCacheServer.GetSlaveInfoAsync: Failed parse info from redis slave, Error:" + ex.Message, ex);
                return null;
            }
        }

        /// <summary>
        /// Promotes the local redis windows service to master, by sending command "SlaveOf No One".
        /// Also changes the writer object (redis client) to write to the new master (local).
        /// We can use the configuration instead of the server ip (127.0.0.1).
        /// </summary>
        /// <param name="slaveInfo">The slave information retrieved with command "info" from local redis windows service.</param>
        /// <param name="redisSlave">The redis slave that is connected to local service.</param>
        /// <param name="currentRedisCacheClient">The current redis cache client which reads-writes.</param>
        /// <returns></returns>
        private async Task PromoteToMaster(RedisSlaveInfo slaveInfo, IRedisNativeClient redisSlave, RedisCacheClient currentRedisCacheClient)
        {
            try
            {
                //change slave to master
                var changeToMasterTask = Task.Run(() => redisSlave.SlaveOfNoOne());

                //get current host or ip while changing to master, this ip should be reachable from the other instances
                var myHost = GetCurrentIp();
                var myPort = _redisSlavePort; //port becaming local, because we will work internal network
                Log.WarnFormat("RedisCacheServer.PromoteToMaster: New master will be (from current slave):{0} {1}", myHost, myPort);

                await changeToMasterTask;

                //lock database record and insert new one 
                Log.Warn("RedisCacheServer.PromoteToMaster: Sending the new master to database");
                await InsertMasterInDatabaseAsync(string.Format("{0} {1}", myHost, myPort), "RedisSlave",
                        string.Format("Master was down for {0} seconds and last IO was {1} seconds ago, change requested from ip:{2}",
                            slaveInfo.MasterLinkDownSinceSeconds, slaveInfo.MasterLastIoSecondsAgo, myHost));
                Log.Warn("RedisCacheServer.PromoteToMaster: Writing to database completed");

                Log.Warn("RedisCacheServer.PromoteToMaster: Setting the current redis cache client to write at the new master (even now we are the master)");
                //setting to local ip, so now the reader and writer is same
                currentRedisCacheClient.SwitchWriterConfigToNewMaster(_redisSlaveHost, _redisSlavePort);
                Log.Warn("RedisCacheServer.PromoteToMaster: Completed setting to the new master.");
            }
            catch (Exception e)
            {
                Log.Error("RedisCacheServer.PromoteToMaster: Failed with error", e);
            }
        }

        /// <summary>
        /// Switches to new master, the local redis instance, and the write object in the redis client.
        /// </summary>
        /// <param name="redisSlave">The redis slave that is installed as windows service to local.</param>
        /// <param name="masterAndPortInDb">The master and port in db.</param>
        /// <param name="currentRedisCacheClient">The current redis cache client which is part of the framework.</param>
        /// <param name="checkIp">if set to <c>true</c> [check ip].</param>
        /// <returns></returns>
        private async Task SwitchToNewMasterAsync(IRedisNativeClient redisSlave, string masterAndPortInDb, RedisCacheClient currentRedisCacheClient, bool checkIp)
        {
            //change master to the new one
            try
            {
                //the database value is deliminated with space, don't change this format, command line tools using also same
                var arrayMasterAndPortInDb = masterAndPortInDb.Split(' ');
                var masterHost = arrayMasterAndPortInDb[0];
                int masterPortAsInteger;

                //trying to parse the integer part, assuming it is format like : "host port"
                if (int.TryParse(arrayMasterAndPortInDb[1], out masterPortAsInteger))
                {
                    //check if the current IP is different than the master ip
                    if (checkIp)
                    {
                        var ip = GetCurrentIp();
                        if (ip.Equals(masterHost, StringComparison.OrdinalIgnoreCase))
                        {
                            //we are the master
                            await Task.Run(() => redisSlave.SlaveOfNoOne());
                            Log.WarnFormat("RedisCacheServer.SwitchToNewMasterAsync: Switched to new master which is CURRENT SERVER (same ip): {0} {1}",
                                masterHost, masterPortAsInteger);
                        }
                        else
                        {
                            //changing the master for our local redis service
                            await Task.Run(() => redisSlave.SlaveOf(masterHost, masterPortAsInteger));
                            Log.WarnFormat("RedisCacheServer.SwitchToNewMasterAsync: Switched to new master: {0} {1}", masterHost, masterPortAsInteger);
                        }
                    }
                    else
                    {
                        //changing the master for our local redis service
                        await Task.Run(() => redisSlave.SlaveOf(masterHost, masterPortAsInteger));
                        Log.WarnFormat("RedisCacheServer.SwitchToNewMasterAsync: Switched to new master: {0} {1}", masterHost, masterPortAsInteger);
                    }

                    //changing the writer object that is connected to the master, so it will write to the new master
                    Log.Warn("RedisCacheServer.SwitchToNewMasterAsync: Setting the current redis cache client to write at the new master");
                    currentRedisCacheClient.SwitchWriterConfigToNewMaster(masterHost, masterPortAsInteger);
                    Log.Warn("RedisCacheServer.SwitchToNewMasterAsync: Completed setting to the new master.");
                }
                else
                {
                    //log exception of bad database record
                    Log.Error("RedisCacheServer.SwitchToNewMasterAsync: Invalid database value to parse: " + masterAndPortInDb);
                }
            }
            catch (Exception ex)
            {
                //log exception of bad database record
                Log.Error("RedisCacheServer.SwitchToNewMasterAsync: Invalid database value to parse: " + masterAndPortInDb, ex);
            }
        }

        #endregion

        #region Database Related Functions

        public string ReadLatestMasterInDatabase() //Sync method only used from outside...
        {
            //connect to database and read last record
            using (var connection = new SqlConnection(_redisMasterConnectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    SqlTransaction transaction = null;

                    try
                    {
                        connection.Open();
                        //only reading the committed last data
                        transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                        command.Transaction = transaction;

                        command.CommandText = SelectStatement;
                        command.Parameters.Add(new SqlParameter("@DeploymentId", RoleEnvironment.DeploymentId));
                        var serverHost = command.ExecuteScalar().ToString().Replace("\r", "").Replace("\n", "").Trim();

                        transaction.Commit(); //although it is select statement, we can commit
                        return serverHost;
                    }
                    catch (Exception ex) //OperationCanceledException, SqlException
                    {
                        Log.Error("RedisCacheServer.ReadLatestMasterInDatabaseAsync: Failed with error.", ex);

                        //other errors. handle it
                        if (transaction != null)
                            transaction.Rollback();
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();
                        connection.Close();
                    }
                }
            }

            return string.Empty;
        }

        private async Task<string> ReadLatestMasterInDatabaseAsync(CancellationToken ct)
        {
            if (ct.IsCancellationRequested)
                return string.Empty;

            //connect to database and read last record
            using (var connection = new SqlConnection(_redisMasterConnectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    SqlTransaction transaction = null;

                    try
                    {
                        connection.Open();
                        //only reading the committed last data
                        transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                        command.Transaction = transaction;

                        command.CommandText = SelectStatement;
                        command.Parameters.Add(new SqlParameter("@DeploymentId", RoleEnvironment.DeploymentId));
                        //if canceled, throw before command is executed
                        ct.ThrowIfCancellationRequested();

                        var serverHost = (await command.ExecuteScalarAsync(ct)).ToString().Replace("\r", "").Replace("\n", "").Trim();

                        transaction.Commit(); //although it is select statement, we can commit
                        return serverHost;
                    }
                    catch (Exception ex) //OperationCanceledException, SqlException
                    {
                        Log.Error("RedisCacheServer.ReadLatestMasterInDatabaseAsync: Failed with error.", ex);

                        //other errors. handle it
                        if (transaction != null)
                            transaction.Rollback();
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();
                        connection.Close();
                    }
                }
            }

            return string.Empty;
        }

        private async Task InsertMasterInDatabaseAsync(string hostAndPort, string insertedBy, string description)
        {
            /*
             * ID (int) - identity, auto
             * ServerHost (varchar-500)
             * InsertedDate (datetime) - default to getdate
             * InsertedBy (varchar 250)
             * Description (varchar max)
             */
            using (var connection = new SqlConnection(_redisMasterConnectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    SqlTransaction transaction = null;

                    try
                    {
                        connection.Open();
                        //lock for writing
                        transaction = connection.BeginTransaction(IsolationLevel.Serializable);
                        command.Transaction = transaction;
                        command.CommandText = UpdateStatement;
                        command.Parameters.AddWithValue("@ServerHost", hostAndPort);
                        command.Parameters.AddWithValue("@InsertedBy", insertedBy);
                        command.Parameters.AddWithValue("@Description", description);
                        command.Parameters.AddWithValue("@DeploymentId", RoleEnvironment.DeploymentId);

                        await command.ExecuteNonQueryAsync();

                        transaction.Commit();
                    }
                    catch (SqlException ex)
                    {
                        Log.Error("RedisCacheServer.InsertMasterInDatabaseAsync: Failed with error.", ex);

                        //other errors. handle it
                        if (transaction != null)
                            transaction.Rollback();
                    }
                    catch (Exception ex)
                    {
                        Log.Error("RedisCacheServer.InsertMasterInDatabaseAsync: Failed with error.", ex);

                        //other errors. handle it
                        if (transaction != null)
                            transaction.Rollback();
                    }
                    finally
                    {
                        if (transaction != null)
                            transaction.Dispose();
                        connection.Close();
                    }
                }
            }
        }

        #endregion

        #region Resolving host name / ip

        /// <summary>
        /// Gets the current ip via azure or from the local file that holds the ip.
        /// First checking the file.
        /// </summary>
        /// <returns></returns>
        public string GetCurrentIp()
        {
            try
            {
                //read from file (it is generated during the deploy)
                //this has higher prio, because it gives flexibility that we can set fixed ip of new redis, for new instances
                Log.WarnFormat("RedisCacheServer.GetCurrentIp: Chceking Ip File {0}", _redisIpFile);
                string ip;
                if (File.Exists(_redisIpFile))
                {
                    ip = File.ReadAllText(_redisIpFile);
                    ip = ip.Replace("\r", "").Replace("\n", "").Replace("\t", ""); //clean invalid chars
                    Log.WarnFormat("RedisCacheServer.GetCurrentIp: Ip retrieved from File [{0}]:{1}", _redisIpFile, ip);
                    return ip;
                }

                Log.WarnFormat("RedisCacheServer.GetCurrentIp: Ip File {0} not found.", _redisIpFile);

                //still cannot find, so read from dns
                ip = GetLocalIp();
                if (!string.IsNullOrEmpty(ip))
                {
                    Log.WarnFormat("RedisCacheServer.GetCurrentIp: Ip retrieved from local dns:{0}", ip);
                    return ip;
                }

                //if file not found, then look at azure instance
                ip = GetCurrentIpFromRoleEnvironment();
                if (!string.IsNullOrEmpty(ip))
                {
                    Log.WarnFormat("RedisCacheServer.GetCurrentIp: Ip retrieved from RoleEnvironment:{0}", ip);
                    return ip;
                }

                //nothing found
                Log.WarnFormat("RedisCacheServer.GetCurrentIp: File [{0}] not found to read the ip, and cannot read from RoleEnvironment.CurrentRoleInstance.InstanceEndpoints or from local dns.",
                    _redisIpFile);
            }
            catch (Exception e)
            {
                Log.Error("RedisCacheServer.GetCurrentIp: Failed with error.", e);
            }

            return string.Empty;
        }

        private static string GetCurrentIpFromRoleEnvironment()
        {
            var ip = string.Empty;
            try
            {
                if (RoleEnvironment.IsAvailable
                    && RoleEnvironment.CurrentRoleInstance != null
                    && RoleEnvironment.CurrentRoleInstance.InstanceEndpoints.Count > 0)
                {
                    //this will be the local ip in any case...
                    ip = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints.First().Value.IPEndpoint.Address.ToString();
                    Log.WarnFormat("RedisCacheServer.GetCurrentIp: Ip retrieved from Azure:{0}", ip);
                }
            }
            catch (Exception ex)
            {
                Log.Error("RedisCacheServer.GetCurrentIp: Cannot retrieve the value from Roleenvironment.", ex);
            }

            return ip;
        }

        /// <summary>
        /// Gets the local ip.
        /// </summary>
        /// <returns></returns>
        private static string GetLocalIp()
        {
            var localIP = "";
            var hostEntry = Dns.GetHostEntry(Dns.GetHostName());

            //it should belong to local internal network
            foreach (var ip in hostEntry.AddressList.Where(ip => ip.AddressFamily == AddressFamily.InterNetwork))
            {
                localIP = ip.ToString();
                break;
            }
            return localIP;
        }

        #endregion
    }
}
