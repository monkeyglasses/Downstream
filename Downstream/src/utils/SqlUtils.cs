using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace com.bitscopic.downstream.utils
{
    public static class SqlUtils
    {
        public static bool hasColumn(IDataReader dr, string columnName)
        {
            for (int i = 0; i < dr.FieldCount; i++)
            {
                if (dr.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
