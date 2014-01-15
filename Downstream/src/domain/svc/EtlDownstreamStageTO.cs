using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.utils;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class EtlDownstreamStageTO : AbstractTO
    {
        public String id;
        public String stage;
        public String startDate;
        public String endDate;
        public EtlDownstreamMapItemTO mapItem;

        public EtlDownstreamStageTO() { }

        public EtlDownstreamStageTO(EtlDownstreamStage domainObj) 
        {
            if (domainObj == null)
            {
                return;
            }
            this.id = domainObj.Id.ToString();
            if (domainObj.MapItem != null)
            {
                this.mapItem = new EtlDownstreamMapItemTO(domainObj.MapItem);
            }
            this.stage = Enum.GetName(typeof(EtlBatchStage), domainObj.Stage);
            this.startDate = DateUtils.getDateTimeString(domainObj.StageStart);
            this.endDate = DateUtils.getDateTimeString(domainObj.StageEnd);
        }
    }

    [Serializable]
    public class EtlDownstreamStageArray : AbstractArrayTO
    {
        public EtlDownstreamStageTO[] stages;

        public EtlDownstreamStageArray() { }

        public EtlDownstreamStageArray(IList<EtlDownstreamStage> stageList)
        {
            if (stageList == null || stageList.Count == 0)
            {
                return;
            }

            this.count = stageList.Count;
            this.stages = new EtlDownstreamStageTO[stageList.Count];
            for (int i = 0; i < stageList.Count; i++)
            {
                this.stages[i] = new EtlDownstreamStageTO(stageList[i]);
            }
        }
    }
}
