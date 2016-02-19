using Baris.Common.Helper;

namespace Baris.Common.Cache.Clients
{
    /// <summary>
    /// This class will be used just to resolve when there is no cache for the second client
    /// </summary>
    public class NoneCacheClient : BaseCacheClient
    {
        #region Properties & Constructor & Dispose

        #endregion

        #region Exists

        public override bool Exists(string key)
        {
            return false;
        } 

        #endregion

        #region Get methods

        public override bool Get<T>(string key, out T value)
        {
            Argument.NotNullOrEmpty(key, "key");
            value = default(T);
            return false;
        }

        #endregion

        #region Set/Add methods, sync functions also calling async, so for redis cache, write operations always async

        public override bool Set<T>(string key, T value, int expiresInMinutes)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");
            return false;
        }

        public override bool Add<T>(string key, T value, int expiresInMinutes)
        {
            Argument.NotNullOrEmpty(key, "key");
            Argument.NotNegativeOrZero(expiresInMinutes, "expiresInMinutes");
            return false;
        }

        #endregion

        #region Multi key operations

        public override void FlushAll()
        {
            //do nothing
        }
        
        #endregion

        #region Expiration

        public override bool IsAboutToExpire(string key)
        {
            return false;
        }

        #endregion
    }
}
