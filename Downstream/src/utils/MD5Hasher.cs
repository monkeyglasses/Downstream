using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Security.Cryptography;

namespace com.bitscopic.downstream.utils
{
    public class MD5Hasher
    {
        /// <summary>
        /// Calculate an MD5 hash on an object
        /// </summary>
        public string calculateMD5(object obj)
        {
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            bf.Serialize(ms, obj);
            byte[] objBytes = ms.ToArray();
            MD5 hasher = System.Security.Cryptography.MD5.Create();
            byte[] hashBytes = hasher.ComputeHash(objBytes);
            ms.Dispose();
            StringBuilder sb = new StringBuilder();
            foreach (byte b in hashBytes)
            {
                sb.Append(b.ToString("x2").ToUpper());
            }
            return sb.ToString();
        }
    }
}
