using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    public class ReportTO : AbstractTO
    {
        public String startTimestamp;
        public String endTimestamp;
        public String text;

        public ReportTO() { }

        public ReportTO(reporting.Report rpt)
        {
            this.startTimestamp = rpt.StartTimestamp.ToString();
            this.endTimestamp = rpt.EndTimestamp.ToString();
            this.text = rpt.ToString();
        }
    }
}
