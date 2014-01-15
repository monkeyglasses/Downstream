using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class AbstractArrayTO : AbstractTO
    {
        public Int32 count;

        public AbstractArrayTO() { }
    }

    [Serializable]
    public class AbstractTO
    {
        public FaultTO fault;

        public AbstractTO() { }
    }

    [Serializable]
    public class FaultTO
    {
        private Exception exc;

        public String innerMessage;
        public String innerStackTrace;
        public String innerType;
        public String message;
        public String stackTrace;
        public String suggestion;
        public String type;

        public FaultTO() { }

        public FaultTO(Exception exc)
        {
            if (exc == null)
            {
                return;
            }
            this.message = exc.Message;
            this.stackTrace = exc.StackTrace;
            if (exc.InnerException != null)
            {
                this.innerMessage = exc.InnerException.Message;
                this.innerStackTrace = exc.InnerException.StackTrace;
            }
        }
    }
}
