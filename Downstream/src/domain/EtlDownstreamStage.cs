using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public class EtlDownstreamStage
    {
        public EtlDownstreamMapItem MapItem { get; set; }

        public EtlBatchStage Stage { get; set; }

        public Int64 Id { get; set; }

        public DateTime StageStart { get; set; }

        public DateTime StageEnd { get; set; }
    }
}
