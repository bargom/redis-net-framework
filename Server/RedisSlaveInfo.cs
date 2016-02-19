using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Baris.Common.Cache.Server
{
    public enum MasterLinkStatus{Up, Down}

    /// <summary>
    /// For parsing the Info of the NativeRedisClient
    /// </summary>
    public class RedisSlaveInfo
    {
        #region Properties

        /// <summary>
        /// Gets or sets the master host.
        /// </summary>
        /// <value>
        /// The master host.
        /// </value>
        public string MasterHost { get; set; }

        /// <summary>
        /// Gets or sets the master port.
        /// </summary>
        /// <value>
        /// The master port.
        /// </value>
        public int MasterPort { get; set; }

        /// <summary>
        /// Gets or sets the master link status.
        /// </summary>
        /// <value>
        /// The master link status.
        /// </value>
        public MasterLinkStatus MasterLinkStatus { get; set; }

        /// <summary>
        /// Gets or sets the master last io seconds ago.
        /// </summary>
        /// <value>
        /// The master last io seconds ago.
        /// </value>
        public int MasterLastIoSecondsAgo { get; set; }

        /// <summary>
        /// Gets or sets the master link down since seconds.
        /// </summary>
        /// <value>
        /// The master link down since seconds.
        /// </value>
        public int MasterLinkDownSinceSeconds { get; set; }

        /// <summary>
        /// Gets the master host and port as combined property.
        /// </summary>
        /// <value>
        /// The master host and port.
        /// </value>
        public string MasterHostAndPort
        {
            get
            {
                //this is how stored in database and used in command line of SlaveOf...
                return string.Format("{0} {1}", MasterHost, MasterPort);
            }
        }

        #endregion

        #region Parser Factory Method

        internal static RedisSlaveInfo Parse(Dictionary<string, string> dictionary)
        {
            /*
             *  WHEN THE SLAVE IS DOWN:
             *  role:slave
                master_host:blabla
                master_port:90
                master_link_status:down
                master_last_io_seconds_ago:-1
                master_sync_in_progress:0
                master_link_down_since_seconds:92
             * 
             *  WHEN THE SLAVE IS UP:
             *  role:slave
                master_host:Baris-dev-redm.cloudapp.net
                master_port:56379
                master_link_status:up
                master_last_io_seconds_ago:1
                master_sync_in_progress:0
             *  
             * */

            return new RedisSlaveInfo()
            {
                MasterHost = GetSafeValue(dictionary, "master_host"),
                MasterPort = GetSafeValueInt(dictionary, "master_port"),
                MasterLinkStatus = GetSafeValue(dictionary, "master_link_status") == "up" ? MasterLinkStatus.Up : MasterLinkStatus.Down,
                MasterLastIoSecondsAgo = GetSafeValueInt(dictionary, "master_last_io_seconds_ago"),
                MasterLinkDownSinceSeconds = GetSafeValueInt(dictionary, "master_link_down_since_seconds")
            };
        }

        #endregion

        #region Parsing Helpers

        private static string GetSafeValue(IReadOnlyDictionary<string, string> dictionary, string key)
        {
            return dictionary.ContainsKey(key) ? dictionary[key] : string.Empty;
        }

        private static int GetSafeValueInt(IReadOnlyDictionary<string, string> dictionary, string key)
        {
            if (!dictionary.ContainsKey(key)) 
                return 0;

            int value;
            return int.TryParse(dictionary[key], out value) ? value : 0;
        }

        #endregion

        #region Overrides

        public override string ToString()
        {
            return string.Format("MasterHost:{0}, MasterPort:{1}, MasterLinkStatus:{2}, MasterLastIoSecondsAgo:{3}, MasterLinkDownSinceSeconds:{4}",
                MasterHost, MasterPort, MasterLinkStatus, MasterLastIoSecondsAgo, MasterLinkDownSinceSeconds);

        }

        #endregion
    }
}
