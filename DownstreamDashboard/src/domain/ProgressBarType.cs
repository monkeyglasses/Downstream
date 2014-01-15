using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace DownstreamDashboard2.domain
{
    public enum ProgressBarType
    {
        Completed_InProgress,
        Completed_Completed,
        Completed_Failed,
        InProgress_NotStarted,
        Failed_NotStarted,
        NotStarted_NotStarted
    }
}