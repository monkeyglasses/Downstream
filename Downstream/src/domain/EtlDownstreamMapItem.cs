using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public class EtlDownstreamMapItem
    {
        public String EtlBatchId { get; set; }

        public String DownstreamBatchId { get; set; }

        public String SiteId { get; set; }

        public override string ToString()
        {
            return String.Format("ETL Batch ID: {0}, Downstream Batch ID: {1}, Site ID: {2}", this.EtlBatchId, this.DownstreamBatchId, this.SiteId);
        }
    }
}
