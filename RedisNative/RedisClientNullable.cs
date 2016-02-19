using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using log4net;
using ServiceStack.Text;
using Baris.Common.Cache.Interfaces;

namespace Baris.Common.Cache.RedisNative
{
    public class RedisClientNullable : ServiceStack.Redis.RedisClient, IRedisClientNullable
    {
        public const string NullValueIndicator = "${<REDISNULLOBJECT>}$";
        public const string NullValueIndicatorWithQuotes = "\"${<REDISNULLOBJECT>}$\"";
        protected static readonly ILog _log = LogManager.GetLogger(typeof(ICacheClient)); //keeping same logger

        public RedisClientNullable(string host, int port)
            : base(host, port)
        {}

        /// <summary>
        /// Gets the specified key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>
        /// True if the key is found.
        /// </returns>
        public bool Get<T>(string key, out T value)
        {
            if (typeof (T) == typeof (byte[]))
            {
                value = (T) (object) base.Get(key);
                return !Equals(value, default(T));
            }

            //the raw value before going into deserialize
            var stringValue = GetValue(key);

            //we dont need to go further if it is null
            if (stringValue == null) //value not found
            {
                value = default(T);
                return false;
            }

            //if it is a null object that was stored with special value indicator, we return null, and return true for key found.
            if (stringValue.Equals(NullValueIndicatorWithQuotes)) //redis adding quotes around
            {
                //value exists but it is null
                value = default(T);
                return true; //the key exists but it is null
            }

            //If T is value type like int, we can try to convert (Now only for numeric types to avoid date time errors)
            if (stringValue.Length > 0 && typeof(T).IsNumericType() && !IsNullableType(typeof(T))) //nullable for deserializer
            {
                //it is value type and has value, convert
                try
                {
                    value = (T) Convert.ChangeType(stringValue, typeof (T));
                    return true;
                }
                catch (Exception)
                {
                    //requested type is not correct
                    value = default(T);
                    _log.ErrorFormat("The requested type and the stored type does not match. Requested Type:{0}, Stored Value:{1}, Key: {2}", typeof(T), stringValue, key);
                    return false;
                }
            }

            //if customized, use that function
            value = JsConfig<T>.HasDeserializeFn ? 
                JsConfig<T>.DeSerializeFn(stringValue) : 
                JsonSerializer.DeserializeFromString<T>(stringValue);

            //if it is nullable type and value is null, it means, value exists
            //so, we can always return true, even if the value can be null
            return true; //value found 
        }

        #region Type Helpers

        bool IsNullableType(Type type)
        {
            // If this is not a value type, it is a reference type, so it is automatically nullable
            //  (NOTE: All forms of Nullable<T> are value types)
            if (!type.IsValueType)
                return true;

            // Report whether an underlying Type exists (if it does, TypeToTest is a nullable Type)
            return Nullable.GetUnderlyingType(type) != null;
        }

        #endregion

        public bool SetAsNull(string key, TimeSpan expire)
        {
            return Set(key, NullValueIndicator, expire);
        }

        public bool AddAsNull(string key, TimeSpan expire)
        {
            return Add(key, NullValueIndicator, expire);
        }
    }
}
