using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class EtlDownstreamMapItemTO
    {
        public String downstreamBatchId;
        public String etlBatchId;
        public String siteId;

        public EtlDownstreamMapItemTO() { }

        public EtlDownstreamMapItemTO(EtlDownstreamMapItem mapItem) 
        {
            if (mapItem == null)
            {
                return;
            }
            this.downstreamBatchId = mapItem.DownstreamBatchId;
            this.etlBatchId = mapItem.EtlBatchId;
            this.siteId = mapItem.SiteId;
        }
    }
}
