using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public static class PortNumberGenerator
    {
        static Int16 current = 2703;
        static readonly object _locker = new object();

        public static Int16 getRandomPort()
        {
            Int16 retVal = 0;
            lock (_locker)
            {
                ++current;
                Guid g = Guid.NewGuid();
                int[] ints = new int[4];
                Buffer.BlockCopy(g.ToByteArray(), 0, ints, 0, 16);
                retVal = Convert.ToInt16(new Random(ints[0]).Next(current, 16000));
            }
            return retVal;
        }
    }
}
