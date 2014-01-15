using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.Compression;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace com.bitscopic.downstream.compression.gzip
{
    public class GZip
    {
        /// <summary>
        /// Compress an object by serializing the data and compressing
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public byte[] compress(object obj)
        {
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            bf.Serialize(ms, obj);

            using (MemoryStream compressedObject = new MemoryStream())
            {
                using (GZipStream gzip = new GZipStream(compressedObject, CompressionMode.Compress, true))
                {
                    gzip.Write(ms.ToArray(), 0, Convert.ToInt32(ms.Length));
                }
                return compressedObject.ToArray();
            }
        }

        /// <summary>
        /// Decompress and deserialize a compressed object
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public object decompress(byte[] bytes)
        {
            using (GZipStream gzip = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress))
            {
                const int bufferSize = 4096;
                byte[] buffer = new byte[bufferSize];
                using (MemoryStream decompressedObject = new MemoryStream())
                {
                    int count = 0;
                    do 
                    {
                        count = gzip.Read(buffer, 0, bufferSize);
                        if (count > 0)
                        {
                            decompressedObject.Write(buffer, 0, count);
                        }
                    }
                    while (count > 0);

                    decompressedObject.Position = 0;
                    BinaryFormatter bf = new BinaryFormatter();
                    object result = bf.Deserialize(decompressedObject);
                    return result;
                }
            }
        }
    }
}
