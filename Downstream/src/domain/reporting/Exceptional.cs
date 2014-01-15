using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.reporting
{
    [Serializable]
    public class Exceptional
    {
        public String Message { get; set; }

        public ErrorCode Code { get; set; }
    }

    [Serializable]
    public enum ErrorCode
    {
        UNDEFINED = 0,
        INFORMATIONAL = -1,
        INVALIDLY_FORMED_RECORD = 1,
        INFINITE_LOOP = 2,
        TRIGGER_NOT_CREATED = 100,
        MANUAL_SHUTDOWN = 200,
        EXCEEDED_MAX_RUN_TIME = 300,
        FILE_IO_ERRORS = 400
    }
}
