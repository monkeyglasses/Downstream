using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace gov.va.medora.downstream.domain.data
{
    /// <summary>
    /// The System.Data.DataTable data structure is convenient to use but consumes large amounts of memory
    /// due to the internal implementation details. We need only a few of the features - this data structure 
    /// will aim to fulfill those requirements while maintaining a low memory footprint
    /// </summary>
    public class DataTableLite
    {
        public String TableName { get; set; }
        public IList<String> DataRow { get; set; }
        public IList<String> ColumnNames { get; set; }


    }
}
