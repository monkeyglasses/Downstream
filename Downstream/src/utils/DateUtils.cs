using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.utils
{
    public static class DateUtils
    {
        /// <summary>
        /// Use this method to ensure all DateTime.ToString calls use the exact same format
        /// </summary>
        /// <returns></returns>
        public static String getDateTimeString(DateTime dt)
        {
            return dt.ToString();
        }

        public static DateTime getDateTimeFromString(String dtString)
        {
            return DateTime.Parse(dtString);
        }
    }
}
