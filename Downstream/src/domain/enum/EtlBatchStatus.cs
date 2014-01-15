using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace com.bitscopic.downstream.domain
{
	public enum EtlBatchStatus
	{
        PROCESSING = 1156860,
        FAILED = 1156861,
        REPROCESSING = 1156862,
        RECOVERED = 1156863,
        COMPLETED = 1156864,
        COMPLETED_WITH_ERRORS = 1156865
	}
}